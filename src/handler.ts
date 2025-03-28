import SuperJSON from "superjson";

import {
  type KeyPart,
  type StreamItem,
  type ValkeyIndexGetter,
  type ValkeyIndexHandler,
  type ValkeyIndexOps,
  type ValkeyIndexSetter,
  type ValkeyIndexStreamHandler,
  type ValkeyIndexSubscriptionHandler,
  type ValkeyIndexUpdater,
} from ".";

export type ValueSerializer<T> = (
  input: T,
) => Record<string, string | number | undefined> | undefined;

export type ValueDeserializer<T> = (
  input: Record<string, string>,
) => T | undefined;

function DEFAULT_SERIALIZER<T>(input: T) {
  if (!input) {
    return {};
  } else if (typeof input === "object") {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [key, SuperJSON.stringify(val)];
      }),
    );
  }
}

function DEFAULT_DESERIALIZER<T>(input: Record<string, string | undefined>) {
  return Object.fromEntries(
    Object.entries(input).map(([key, val]) => {
      return [key, val ? SuperJSON.parse(val) : val];
    }),
  ) as T;
}

export function assembleRecord(fields: string[]) {
  const r: Record<string, string> = {};
  for (let i = 0; i + 2 <= fields.length; i += 2) {
    r[fields[i]!] = fields[i + 1]!;
  }
  return r;
}

export function relatedHash<T, R extends keyof T>({ fields }: { fields: R[] }) {
  return function related(value: Partial<T>) {
    const results = Object.fromEntries(
      Object.entries(value)
        ?.filter(([field]) => fields.findIndex((x) => x === field) !== -1)
        ?.map(([field, fval]) => {
          if (Array.isArray(fval)) {
            return [field, fval.map((x) => String(x))];
          }
          return [field, String(fval)];
        }) ?? [],
    );
    return results as Record<R, KeyPart | KeyPart[] | undefined>;
  };
}

export function getHash<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}): ValkeyIndexGetter<T, any> {
  return async function get({ valkey }, key) {
    const value = await valkey.hgetall(key);
    return convert(value);
  };
}

export function setHash<T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}): ValkeyIndexSetter<T, any> {
  return async function set(_ops, pipeline, { key, input }) {
    if (input === undefined) {
      return;
    }
    const value = convert(input);
    if (value === undefined) {
      return;
    }
    pipeline.del(key);
    pipeline.hset(key, value);
  };
}

export function updateHash<T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<Partial<T>>;
} = {}): ValkeyIndexUpdater<T, any> {
  return async function update(_ops, pipeline, { key, input }) {
    const value = convert(input);
    if (value === undefined) {
      return;
    }
    for (const [field, field_value] of Object.entries(value)) {
      if (field_value === undefined) {
        continue;
      }
      pipeline.hset(key, field, field_value);
    }
  };
}

export function appendStream<T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}): ValkeyIndexHandler<
  {
    pkey: string;
    input: T | undefined;
    id?: string;
  },
  T,
  any
> {
  return async function append(
    { valkey, toKey, touch, ttl = null, maxlen = null },
    { pkey, input, id },
    options,
  ) {
    const key = toKey(pkey);
    const value = input && convert(input);
    if (value === undefined) {
      return;
    }
    const pipeline = valkey.multi();
    if (ttl !== null) {
      const [seconds] = (await valkey.time()) as [number, number];
      pipeline.xtrim(key, "MINID", "~", (seconds - ttl) * 1000);
    }
    if (maxlen !== null) {
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
    await touch(pipeline, { pkey, value: input }, options);
    await pipeline.exec();
  };
}

export function rangeStream<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}): ValkeyIndexStreamHandler<
  { pkey: string; start?: string; stop?: string },
  T,
  any
> {
  return async function range(
    { valkey, toKey, touch, ttl = null, maxlen = null },
    { pkey, start, stop },
    options,
  ) {
    const key = toKey(pkey);
    const pipeline = valkey.multi();
    if (ttl !== null) {
      const [seconds] = (await valkey.time()) as [number, number];
      pipeline.xtrim(key, "MINID", "~", (seconds - ttl) * 1000);
    }
    if (maxlen !== null) {
      pipeline.xtrim(key, "MAXLEN", "~", maxlen);
    }
    pipeline.xrange(key, start ?? "-", stop ?? "+");
    await touch(pipeline, { pkey }, options);
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

// HOPE bun fixes https://github.com/oven-sh/bun/issues/17591
// until then this leaks connections
export function readStream<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}): ValkeyIndexSubscriptionHandler<
  { pkey: string; lastId?: string; signal?: AbortSignal },
  T,
  any
> {
  return async function* read(
    ops,
    { pkey, lastId, signal },
    options,
  ): AsyncGenerator<StreamItem<T | undefined>> {
    const { valkey, toKey, touch, ttl = null, maxlen = null } = ops;
    const key = toKey(pkey);
    const pipeline = valkey.multi();
    if (ttl !== null) {
      const [seconds] = (await valkey.time()) as [number, number];
      pipeline.xtrim(key, "MINID", "~", (seconds - ttl) * 1000);
    }
    if (maxlen !== null) {
      pipeline.xtrim(key, "MAXLEN", "~", maxlen);
    }
    await touch(pipeline, { pkey }, options);
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
        const results = await subscription.xread(
          "BLOCK",
          0,
          "STREAMS",
          key,
          lastSeen,
        );
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
