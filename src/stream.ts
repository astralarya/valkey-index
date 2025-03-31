import { type KeyPart, type ValkeyIndexerReturn } from ".";
import {
  assembleRecord,
  DEFAULT_DESERIALIZER,
  DEFAULT_SERIALIZER,
  type ValueDeserializer,
  type ValueSerializer,
} from "./serde";

export function appendStream<T, R extends keyof T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}) {
  return async function append(
    { valkey, key: _key, touch, ttl, maxlen }: ValkeyIndexerReturn<T, R>,
    {
      pkey,
      input,
      id,
      ttl: ttl_,
      message,
    }: { pkey: KeyPart; input: T; id: string; ttl?: number; message: string },
  ) {
    const key = _key({ pkey });
    const value = input && convert(input);
    if (value === undefined) {
      return;
    }
    const pipeline = valkey.multi();
    if (ttl !== undefined) {
      const [seconds] = (await valkey.time()) as [number, number];
      pipeline.xtrim(key, "MINID", "~", (seconds - ttl) * 1000);
    }
    if (maxlen !== undefined) {
      pipeline.xtrim(key, "MAXLEN", "~", maxlen);
    }
    pipeline.xadd(
      key,
      id ?? "*",
      ...Array.from(
        Object.entries(value).filter(
          (item): item is [(typeof item)[0], NonNullable<(typeof item)[1]>] =>
            input !== undefined,
        ),
      ).flat(),
    );
    await touch(pipeline, { pkey, value: input, ttl: ttl_, message });
    await pipeline.exec();
  };
}

export function rangeStream<T, R extends keyof T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}) {
  return async function range(
    { valkey, key: _key, touch, ttl, maxlen }: ValkeyIndexerReturn<T, R>,
    {
      pkey,
      start,
      stop,
      ttl: ttl_,
      message,
    }: {
      pkey: KeyPart;
      start?: string;
      stop?: string;
      ttl?: number;
      message: string;
    },
  ) {
    const key = _key({ pkey });
    const pipeline = valkey.multi();
    if (ttl !== undefined) {
      const [seconds] = (await valkey.time()) as [number, number];
      pipeline.xtrim(key, "MINID", "~", (seconds - ttl) * 1000);
    }
    if (maxlen !== undefined) {
      pipeline.xtrim(key, "MAXLEN", "~", maxlen);
    }
    pipeline.xrange(key, start ?? "-", stop ?? "+");
    await touch(pipeline, { pkey, ttl: ttl_, message });
    const results = await pipeline.exec();
    const result = results?.[0]?.[1] as [string, string[]][];
    return result.map(([id, fields]) => {
      return {
        id,
        data: convert(assembleRecord(fields)),
      };
    });
  };
}

export type StreamItem<T> = {
  id: string;
  data: T;
};

// HOPE bun fixes https://github.com/oven-sh/bun/issues/17591
// until then this leaks connections
export function readStream<T, R extends keyof T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}) {
  return async function* read(
    ops: ValkeyIndexerReturn<T, R>,
    {
      pkey,
      count,
      block,
      lastId,
      signal,
      ttl: ttl_,
      message,
    }: {
      pkey: KeyPart;
      count?: number;
      block?: number;
      lastId?: string;
      signal?: AbortSignal;
      ttl?: number;
      message: string;
    },
  ): AsyncGenerator<StreamItem<T | undefined>> {
    const { valkey, key: _key, touch, ttl = null, maxlen = null } = ops;
    const key = _key({ pkey });
    const pipeline = valkey.multi();
    if (ttl !== null) {
      const [seconds] = (await valkey.time()) as [number, number];
      pipeline.xtrim(key, "MINID", "~", (seconds - ttl) * 1000);
    }
    if (maxlen !== null) {
      pipeline.xtrim(key, "MAXLEN", "~", maxlen);
    }
    await touch(pipeline, { pkey, ttl: ttl_, message });
    await pipeline.exec();
    const subscription = valkey.duplicate();
    // console.log("signal", signal);
    const abort = signal?.addEventListener("abort", () => {
      // console.log("ABORT");
      subscription.disconnect();
      signal.removeEventListener("abort", abort!);
    });
    let lastSeen: string = lastId ?? "$";
    try {
      while (!signal?.aborted) {
        // console.log("BLOCKING");
        const args = [
          ...(count !== undefined ? ["COUNT", count] : []),
          ...["BLOCK", block ?? 0],
          "STREAMS",
          key,
          lastSeen,
        ] as unknown as Parameters<(typeof subscription)["xread"]>;
        const results = await subscription.xread(...args);
        for (const [, items] of results ?? []) {
          for (const [id, fields] of items) {
            lastSeen = id;
            const data = convert(assembleRecord(fields));
            if (data) {
              yield { id, data };
            }
          }
        }
      }
    } catch (e) {
      console.error(e);
    } finally {
      // console.log("REMOVING");
      subscription.disconnect();
      signal?.removeEventListener("abort", abort!);
    }
    return () => {
      // console.log("REMOVING");
      subscription.disconnect();
      signal?.removeEventListener("abort", abort!);
    };
  };
}
