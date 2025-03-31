import { bindHandlers, type ValkeyIndexSpec } from "./handler";
import {
  ValkeyIndexer,
  type KeyPart,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
} from "./indexer";
import {
  assembleRecord,
  DEFAULT_DESERIALIZER,
  DEFAULT_SERIALIZER,
  type ValueDeserializer,
  type ValueSerializer,
} from "./serde";

export type ValkeyStreamIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeyStreamIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  exemplar: T | 0;
  functions?: F;
  serializer?: ValueSerializer<T>;
  deserializer?: ValueDeserializer<T>;
};

export type ValkeyStreamIndexOps<T> = {};

export function ValkeyStreamIndex<
  T,
  F extends ValkeyIndexSpec<ValkeyStreamIndexOps<T>>,
>({
  valkey,
  name,
  ttl,
  maxlen,
  functions = {} as F,
  serializer,
  deserializer,
}: ValkeyStreamIndexProps<T, F>) {
  const append_ = appendStream({ convert: serializer });
  const range_ = rangeStream({ convert: deserializer });
  const read_ = readStream({ convert: deserializer });

  const indexer = ValkeyIndexer<T, never>({
    valkey,
    name,
    ttl,
    maxlen,
  });

  function append(arg: AppendStreamArg<T>) {
    append_(indexer, arg);
  }

  async function range(arg: RangeStreamArg) {
    return range_(indexer, arg);
  }

  async function read(arg: ReadStreamArg) {
    return read_(indexer, arg);
  }

  const ops = {
    ...indexer,
    append,
    range,
    read,
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
  };
}

export type AppendStreamArg<T> = {
  pkey: KeyPart;
  input: T;
  id?: string;
  ttl?: number;
  message?: string;
};

export function appendStream<T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}) {
  return async function append(
    { valkey, key: _key, touch, ttl, maxlen }: ValkeyIndexerReturn<T, never>,
    { pkey, input, id, ttl: ttl_, message }: AppendStreamArg<T>,
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

type RangeStreamArg = {
  pkey: KeyPart;
  start?: string;
  stop?: string;
  ttl?: number;
  message?: string;
};

export function rangeStream<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}) {
  return async function range(
    { valkey, key: _key, touch, ttl, maxlen }: ValkeyIndexerReturn<T, never>,
    { pkey, start, stop, ttl: ttl_, message }: RangeStreamArg,
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

export type ValkeyStreamItem<T> = {
  id: string;
  data: T;
};

type ReadStreamArg = {
  pkey: KeyPart;
  count?: number;
  block?: number;
  lastId?: string;
  signal?: AbortSignal;
  ttl?: number;
  message?: string;
};

// HOPE bun fixes https://github.com/oven-sh/bun/issues/17591
// until then this leaks connections
export function readStream<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}) {
  return async function* read(
    ops: ValkeyIndexerReturn<T, never>,
    { pkey, count, block, lastId, signal, ttl: ttl_, message }: ReadStreamArg,
  ): AsyncGenerator<ValkeyStreamItem<T | undefined>> {
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
