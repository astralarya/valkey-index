import { on } from "events";
import type Redis from "iovalkey";
import { type ChainableCommander } from "iovalkey";

export * from "./handler";

export const DEFAULT_TTL = 60 * 60 * 24;
export const DEFAULT_MAXLEN = 8;

export const VALKEY_INDEX_NAME_REGEX = /^[a-zA-Z0-9_\.\/]+$/;

export type KeyPart = string | number | symbol;

export type ValkeyIndexOptions<T, R extends keyof T> = {
  valkey: Redis;
  name: string;
  exemplar: T | 0;
  relations: R[];
  get?: ValkeyIndexGetter<T, R>;
  set?: ValkeyIndexSetter<T, R>;
  update?: ValkeyIndexUpdater<T, R>;
  ttl?: number | null;
  maxlen?: number | null;
};

export type ValkeyIndexGetter<T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  key: string,
) => Promise<T | undefined>;

export type ValkeyIndexSetter<T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  pipeline: ChainableCommander,
  arg: { key: string; input: T },
  options?: ValkeyIndexHandlerOptions,
) => Promise<void> | void;

export type ValkeyIndexUpdater<T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  pipeline: ChainableCommander,
  arg: { key: string; input: Partial<T> },
  options?: ValkeyIndexHandlerOptions,
) => Promise<void> | void;

export type ValkeyIndexHandlerOptions = {
  ttl?: Date | number;
  message?: string;
};

export type ValkeyIndexOps<T, R extends keyof T> = {
  valkey: Redis;
  name: string;
  relations: R[];
  ttl?: number | null;
  maxlen?: number | null;
  toKey: (id: KeyPart, relation?: R) => string;
  pkeysVia: (relation: R, fkey: KeyPart) => Promise<string[]>;
  related: (
    value: Partial<T>,
  ) => Record<R, KeyPart[] | KeyPart | undefined> | undefined;
  get?: (pkey: string) => Promise<T | undefined>;
  set?: (
    arg: { pkey: string; input: T },
    options: ValkeyIndexHandlerOptions,
  ) => Promise<void>;
  update?: (
    arg: { pkey: string; input: Partial<T> },
    options?: ValkeyIndexHandlerOptions,
  ) => Promise<void>;
  touch: (
    pipeline: ChainableCommander,
    arg: {
      pkey: KeyPart;
      value?: Partial<T>;
    },
    options?: ValkeyIndexHandlerOptions,
  ) => Promise<void>;
  subscribe: (x: {
    pkey: KeyPart;
    signal?: AbortSignal | undefined;
    test?: string | RegExp;
  }) => AsyncGenerator<string>;
  subscribeVia: (x: {
    fkey: KeyPart;
    relation: R;
    signal?: AbortSignal | undefined;
    test?: string | RegExp;
  }) => AsyncGenerator<string>;
  del: (pkey: KeyPart) => Promise<void>;
  delVia: (relation: R, fkey: KeyPart) => Promise<void>;
};

export type ValkeyIndexCommand<A, T> = (
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<T>;
export type ValkeyIndexHandler<A, T, R extends keyof T, V> = (
  ops: ValkeyIndexOps<T, R>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => V;

export type ValkeyIndexSpec<T, R extends keyof T> = Record<
  string,
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  ValkeyIndexHandler<any, T, R, any>
>;

export type ValkeyIndexFunction<
  T,
  R extends keyof T,
  M extends ValkeyIndexSpec<T, R>,
  K extends keyof M,
> = M[K] extends ValkeyIndexHandler<infer A, T, R, infer V>
  ? (arg: A, options?: ValkeyIndexHandlerOptions) => Promise<V>
  : never;

export type ValkeyIndexInterface<
  T,
  R extends keyof T,
  M extends ValkeyIndexSpec<T, R>,
> = {
  [K in keyof M]: ValkeyIndexFunction<T, R, M, K>;
};

export type StreamItem<T> = {
  id: string;
  data: T;
};

export function calculateRm<T, R extends keyof T>({
  curr,
  next,
}: {
  curr?: Record<R, KeyPart | KeyPart[] | undefined>;
  next?: Record<R, unknown | unknown[] | undefined>;
}) {
  return curr
    ? (Object.fromEntries(
        (Object.entries(curr) as [R, KeyPart | KeyPart[] | undefined][]).map(
          ([relation, fkeys]): [R, KeyPart | KeyPart[] | undefined] => {
            if (!next || !next[relation]) {
              return [relation, fkeys];
            }
            const newkeys = next[relation];
            if (!fkeys) {
              return [relation, undefined];
            } else if (Array.isArray(fkeys)) {
              return [
                relation,
                Array.isArray(newkeys)
                  ? fkeys.filter(
                      (x) => newkeys.find((y) => x === String(y)) === -1,
                    )
                  : fkeys.filter((x) => x !== String(newkeys)),
              ];
            } else {
              if (Array.isArray(newkeys)) {
                return [
                  relation,
                  newkeys.find((y) => fkeys === String(y)) === -1
                    ? fkeys
                    : undefined,
                ];
              } else {
                return [
                  relation,
                  String(newkeys) !== fkeys ? fkeys : undefined,
                ];
              }
            }
          },
        ),
      ) as Record<R, KeyPart | KeyPart[] | undefined>)
    : undefined;
}

export function createValkeyIndex<
  T,
  R extends keyof T,
  M extends ValkeyIndexSpec<T, R>,
>(
  index: ValkeyIndexOptions<T, R>,
  functions?: M,
): Omit<ValkeyIndexOps<T, R>, "get" | "set" | "update"> & {
  get: (typeof index)["get"] extends undefined
    ? undefined
    : (pkey: string) => Promise<T | undefined>;
  set: (typeof index)["set"] extends undefined
    ? undefined
    : (
        arg: { pkey: string; input: T },
        options?: ValkeyIndexHandlerOptions,
      ) => Promise<void>;
  update: (typeof index)["update"] extends undefined
    ? undefined
    : (
        arg: { pkey: string; input: Partial<T> },
        options?: ValkeyIndexHandlerOptions,
      ) => Promise<void>;
  func: ValkeyIndexInterface<T, R, M>;
  f: ValkeyIndexInterface<T, R, M>;
  append: "append" extends keyof M
    ? ValkeyIndexFunction<T, R, M, "append">
    : undefined;
  range: "range" extends keyof M
    ? ValkeyIndexFunction<T, R, M, "range">
    : undefined;
  read: "read" extends keyof M
    ? ValkeyIndexFunction<T, R, M, "read">
    : undefined;
};

export function createValkeyIndex<
  T,
  R extends keyof T,
  M extends ValkeyIndexSpec<T, R>,
>(
  {
    valkey,
    name,
    relations,
    get: get_,
    set: set_,
    update: update_,
    ttl = DEFAULT_TTL,
    maxlen = DEFAULT_MAXLEN,
  }: ValkeyIndexOptions<T, R>,
  functions = {} as M,
) {
  if (!VALKEY_INDEX_NAME_REGEX.test(name)) {
    throw Error(
      "valkey-index: name may only contain alphanumeric, underscore, and dots",
    );
  }
  if (relations.find((x) => !VALKEY_INDEX_NAME_REGEX.test(String(x)))) {
    throw Error(
      "valkey-index: related names may only contain alphanumeric, underscore, and dots",
    );
  }

  function toKey(id: KeyPart, relation?: R) {
    if (relation) {
      return `${name}@${String(relation)}:${String(id)}`;
    } else {
      return `${name}:${String(id)}`;
    }
  }

  function related(value: Partial<T>) {
    const results = Object.fromEntries(
      Object.entries(value)
        ?.filter(([field]) => relations?.findIndex((x) => x === field) !== -1)
        ?.map(([field, fval]) => {
          if (Array.isArray(fval)) {
            return [field, fval.map((x) => String(x))];
          }
          return [field, String(fval)];
        }) ?? [],
    );
    return results as Record<R, KeyPart | KeyPart[] | undefined>;
  }

  async function pkeysVia(relation: R, fkey: KeyPart) {
    const key = toKey(fkey, relation);
    return valkey.zrange(key, 0, "-1");
  }

  async function get(pkey: KeyPart, options?: ValkeyIndexHandlerOptions) {
    if (!get_) {
      throw Error("valkey-index: get() invoked without being defined");
    }
    const key = toKey(pkey);
    const value = await get_(ops, key);
    const pipeline = valkey.multi();
    await touch(pipeline, { pkey, value }, options);
    await pipeline.exec();
    return value;
  }

  async function set(
    { pkey, input }: { pkey: KeyPart; input: T },
    options?: ValkeyIndexHandlerOptions,
  ) {
    if (!get_) {
      throw Error("valkey-index: set() invoked without get() being defined");
    }
    if (!set_) {
      throw Error("valkey-index: set() invoked without being defined");
    }
    const key = toKey(pkey);
    const curr_value = await get_(ops, key);
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const rm = calculateRm<T, R>({ curr, next });
    const pipeline = valkey.multi();
    const value = await set_(ops, pipeline, { key, input }, options);
    await touch(pipeline, { pkey, value: input, rm }, options);
    await pipeline.exec();
    return value;
  }

  async function update(
    { pkey, input }: { pkey: KeyPart; input: Partial<T> },
    options?: ValkeyIndexHandlerOptions,
  ) {
    if (!get_) {
      throw Error("valkey-index: update() invoked without get() being defined");
    }
    if (!update_) {
      throw Error("valkey-index: update() invoked without being defined");
    }
    const key = toKey(pkey);
    const curr_value = await get_!(ops, key);
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const rm = calculateRm<T, R>({ curr, next });
    const pipeline = valkey.multi();
    const value = await update_!(ops, pipeline, { key, input }, options);
    await touch(pipeline, { pkey, value: input, rm }, options);
    await pipeline.exec();
    return value;
  }

  function touchRelated(
    pipeline: ChainableCommander,
    relation: R,
    pkey: KeyPart,
    fkey: KeyPart,
    { ttl: ttl_in, message }: ValkeyIndexHandlerOptions,
  ) {
    const key = toKey(fkey, relation);
    if (ttl_in instanceof Date) {
      pipeline.zadd(key, ttl_in.valueOf(), String(pkey));
    } else if (typeof ttl_in === "number") {
      pipeline.zadd(key, Date.now() + ttl_in, String(pkey));
    } else if (ttl !== null) {
      pipeline.zadd(key, Date.now() + ttl, String(pkey));
    } else {
      pipeline.zadd(key, "+inf", String(pkey));
    }
    if (ttl !== null) {
      pipeline.expire(key, ttl);
    }
    pipeline.zremrangebyscore(key, "-inf", Date.now());
    if (maxlen !== null) {
      pipeline.zremrangebyrank(key, 0, -maxlen - 1);
    }
    if (message !== undefined) {
      pipeline.publish(key, message);
    }
  }

  async function touch(
    pipeline: ChainableCommander,
    {
      pkey,
      value,
      rm,
    }: {
      pkey: KeyPart;
      value?: Partial<T>;
      rm?: Record<R, KeyPart[] | KeyPart | undefined>;
    },
    { ttl: ttl_in, message }: ValkeyIndexHandlerOptions = {},
  ) {
    const key = toKey(pkey);
    const exists = (await valkey.exists(key)) !== 0;
    const relations = value && related ? related(value) : undefined;
    if (ttl_in instanceof Date) {
      pipeline.expireat(key, ttl_in.valueOf());
    } else if (typeof ttl_in === "number") {
      pipeline.expire(key, ttl_in);
    } else if (ttl) {
      pipeline.expire(key, ttl);
    }
    if (message !== undefined) {
      pipeline.publish(key, message);
    }
    if (rm) {
      for (const [relation, fkey] of Object.entries(rm) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            pipeline.zrem(toKey(item as KeyPart, relation), String(pkey));
          });
        } else if (fkey !== undefined) {
          pipeline.zrem(toKey(fkey as KeyPart, relation), String(pkey));
        }
      }
    }
    if (relations) {
      // if (!exists) {
      //   for (const [relation, fkey] of Object.entries(relations) as [
      //     R,
      //     KeyPart | KeyPart[] | undefined,
      //   ][]) {
      //     if (Array.isArray(fkey)) {
      //       fkey.forEach((item) => {
      //         pipeline.zrem(toKey(item as KeyPart, relation), String(pkey));
      //       });
      //     } else if (fkey !== undefined) {
      //       pipeline.zrem(toKey(fkey as KeyPart, relation), String(pkey));
      //     }
      //   }
      // } else {
      for (const [relation, fkey] of Object.entries(relations) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            touchRelated(pipeline, relation, pkey, item as KeyPart, {
              ttl: ttl_in,
              message,
            });
          });
        } else if (fkey !== undefined) {
          touchRelated(pipeline, relation, pkey, fkey as KeyPart, {
            ttl: ttl_in,
            message,
          });
        }
      }
    }
  }

  // HOPE bun fixes https://github.com/oven-sh/bun/issues/17591
  // until then this leaks connections
  async function* subscribe({
    pkey,
    signal,
    test,
  }: {
    pkey: KeyPart;
    signal?: AbortSignal;
    test?: string | RegExp;
  }) {
    if (signal?.aborted) {
      return;
    }
    const key = toKey(pkey);
    const subscription = valkey.duplicate();
    await subscription.subscribe(key);
    for await (const [channel, message] of on(subscription, "message", {
      signal,
    }) as AsyncGenerator<[string, string]>) {
      if (channel === key) {
        if (test === undefined) {
          yield message;
        } else if (test instanceof RegExp) {
          if (test.test(message)) {
            yield message;
          }
        } else if (message === test) {
          yield message;
        }
      }
    }
  }

  // HOPE bun fixes https://github.com/oven-sh/bun/issues/17591
  // until then this leaks connections
  async function* subscribeVia({
    fkey,
    relation,
    signal,
    test,
  }: {
    fkey: KeyPart;
    relation: R;
    signal?: AbortSignal;
    test?: string | RegExp;
  }) {
    if (signal?.aborted) {
      return;
    }
    const key = toKey(fkey, relation);
    const subscription = valkey.duplicate();
    await subscription.subscribe(key);
    for await (const [channel, message] of on(subscription, "message", {
      signal,
    }) as AsyncGenerator<[string, string]>) {
      if (channel === key) {
        if (test === undefined) {
          yield message;
        } else if (test instanceof RegExp) {
          if (test.test(message)) {
            yield message;
          }
        } else if (message === test) {
          yield message;
        }
      }
    }
  }

  async function del(pkey: KeyPart) {
    const key = toKey(pkey);
    const value = get ? await get(key) : undefined;
    const relations = value && related ? related(value) : null;
    const pipeline = valkey.multi();
    if (relations) {
      for (const [relation, fkey] of Object.entries(relations) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            pipeline.zrem(toKey(item as KeyPart, relation), String(pkey));
          });
        } else if (fkey !== undefined) {
          pipeline.zrem(toKey(fkey as KeyPart, relation), String(pkey));
        }
      }
    }
    pipeline.del(key);
    await pipeline.exec();
  }

  async function delVia(relation: R, fkey: KeyPart) {
    const key = toKey(fkey, relation);
    const pkeys = await valkey.zrange(key, 0, "-1");
    await valkey.del(key);
    await Promise.all(pkeys.map((pkey) => del(pkey)));
  }

  const ops: ValkeyIndexOps<T, R> = {
    valkey,
    name,
    relations,
    toKey,
    pkeysVia,
    ...{ get, set, update },
    related,
    touch,
    subscribe,
    subscribeVia,
    del,
    delVia,
  };

  const func = Object.entries(functions).reduce((prev, [key, val]) => {
    prev[key as keyof M] = ((
      arg: Parameters<ValkeyIndexFunction<T, R, M, typeof key>>[1],
      options?: ValkeyIndexHandlerOptions,
    ) => {
      return val(ops, arg, options);
    }) as ValkeyIndexFunction<T, R, M, typeof key>;
    return prev;
  }, {} as ValkeyIndexInterface<T, R, M>);

  return {
    ...ops,
    func,
    f: func,
    append: func.append,
    range: func.range,
    read: func.read,
  };
}
