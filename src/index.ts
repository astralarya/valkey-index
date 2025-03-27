import { on } from "events";
import type Redis from "iovalkey";
import { type ChainableCommander } from "iovalkey";

export * from "./handler";

export const DEFAULT_TTL = 60 * 60 * 24;
export const DEFAULT_MAXLEN = 8;

export type KeyPart = string | number | symbol;

export type ValkeyIndex<T, R extends keyof T> = {
  valkey: Redis;
  // name/relation limited to alphanum, underscore, dot
  name: string;
  get?: ValkeyIndexGetter<T, R>;
  set?: ValkeyIndexSetter<T, R>;
  related?: (
    value: Partial<T>,
  ) => Record<R, KeyPart[] | KeyPart | undefined> | undefined;
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
) => void;

export type ValkeyIndexHandlerOptions = {
  ttl?: Date | number;
  message?: string;
};

export type ValkeyIndexOps<T, R extends keyof T> = {
  valkey: Redis;
  ttl?: number | null;
  maxlen?: number | null;
  toKey: (id: KeyPart, relation?: string) => string;
  pkeysVia: (relation: string, fkey: KeyPart) => Promise<string[]>;
  get?: (pkey: string) => Promise<T | undefined>;
  set?: (
    arg: { pkey: string; input: T },
    options: ValkeyIndexHandlerOptions,
  ) => Promise<void>;
  related: (
    value: Partial<T>,
  ) => Record<R, KeyPart[] | KeyPart | undefined> | undefined;
  touch: (
    pipeline: ChainableCommander,
    arg: {
      pkey: KeyPart;
      value?: Partial<T>;
    },
    options?: ValkeyIndexHandlerOptions,
  ) => Promise<number>;
  subscribe: (x: {
    pkey: KeyPart;
    signal?: AbortSignal | undefined;
    test?: string | RegExp;
  }) => AsyncIterable<string>;
  subscribeVia: (x: {
    fkey: KeyPart;
    relation: string;
    signal?: AbortSignal | undefined;
    test?: string | RegExp;
  }) => AsyncIterable<string>;
  del: (pkey: KeyPart) => Promise<void>;
  delVia: (relation: string, fkey: KeyPart) => Promise<void>;
};

export type ValkeyIndexCommand<A, T> = (
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<T>;
export type ValkeyIndexHandler<A, T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<void>;
export type ValkeyIndexItemHandler<A, T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<T>;
export type ValkeyIndexStreamHandler<A, T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<StreamItem<T | undefined>[]>;
export type ValkeyIndexSubscriptionHandler<A, T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => AsyncGenerator<StreamItem<T | undefined>>;

export type ValkeyIndexSpec<T, R extends keyof T> = Record<
  string,
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexHandler<any, T, R>
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexItemHandler<any, T, R>
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexStreamHandler<any, T, R>
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexSubscriptionHandler<any, T, R>
>;

export type ValkeyIndexFunction<
  T,
  R extends keyof T,
  M extends ValkeyIndexSpec<T, R>,
  K extends keyof M,
> = M[K] extends ValkeyIndexHandler<infer A, T, R>
  ? (arg: A, options?: ValkeyIndexHandlerOptions) => Promise<void>
  : M[K] extends ValkeyIndexItemHandler<infer A, T, R>
  ? (arg: A, options?: ValkeyIndexHandlerOptions) => Promise<T>
  : M[K] extends ValkeyIndexStreamHandler<infer A, T, R>
  ? (
      arg: A,
      options?: ValkeyIndexHandlerOptions,
    ) => Promise<StreamItem<T | undefined>[]>
  : M[K] extends ValkeyIndexSubscriptionHandler<infer A, T, R>
  ? (
      arg: A,
      options?: ValkeyIndexHandlerOptions,
    ) => Promise<AsyncGenerator<StreamItem<R | undefined>>>
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

export function calculateRm<R extends KeyPart>({
  curr,
  next,
}: {
  curr?: Record<R, KeyPart | KeyPart[] | undefined>;
  next?: Record<R, unknown | unknown[] | undefined>;
}) {
  return curr
    ? Object.fromEntries(
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
      )
    : undefined;
}

export function createValkeyIndex<
  T,
  R extends keyof T,
  M extends ValkeyIndexSpec<T, R>,
>(
  index: ValkeyIndex<T, R>,
  functions?: M,
): Omit<ValkeyIndexOps<T, R>, "get"> & {
  get: (typeof index)["get"] extends undefined
    ? undefined
    : (pkey: string) => Promise<T | undefined>;
  set: (typeof index)["set"] extends undefined
    ? undefined
    : (
        arg: { pkey: string; input: T },
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
    get: get_,
    set: set_,
    related = () => ({} as Record<R, string>),
    ttl = DEFAULT_TTL,
    maxlen = DEFAULT_MAXLEN,
  }: ValkeyIndex<T, R>,
  functions = {},
) {
  function toKey(id: KeyPart, relation?: string) {
    if (relation) {
      return `${name}/${relation}:${String(id)}`;
    } else {
      return `${name}:${String(id)}`;
    }
  }

  async function pkeysVia(relation: string, fkey: KeyPart) {
    const key = toKey(fkey, relation);
    return valkey.zrange(key, 0, "-1");
  }

  async function get(pkey: KeyPart, options?: ValkeyIndexHandlerOptions) {
    if (!get_) {
      throw "valkey-index: get() invoked without being defined";
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
    if (!set_) {
      throw "valkey-index: get() invoked without being defined";
    }
    const key = toKey(pkey);
    const pipeline = valkey.multi();
    const value = set_(ops, pipeline, { key, input }, options);
    await touch(pipeline, { pkey, value: input }, options);
    await pipeline.exec();
    return value;
  }

  function touchRelated(
    pipeline: ChainableCommander,
    relation: string,
    pkey: KeyPart,
    fkey: KeyPart,
    { ttl: ttl_in, message }: ValkeyIndexHandlerOptions,
  ) {
    let exec_count = 0;
    const key = toKey(fkey, relation);
    if (ttl_in instanceof Date) {
      pipeline.zadd(key, ttl_in.valueOf(), String(pkey));
      exec_count += 1;
    } else if (typeof ttl_in === "number") {
      pipeline.zadd(key, Date.now() + ttl_in, String(pkey));
      exec_count += 1;
    } else if (ttl !== null) {
      pipeline.zadd(key, Date.now() + ttl, String(pkey));
      exec_count += 1;
    } else {
      pipeline.zadd(key, "+inf", String(pkey));
      exec_count += 1;
    }
    if (ttl !== null) {
      pipeline.expire(key, ttl);
      exec_count += 1;
    }
    if (message !== undefined) {
      pipeline.publish(key, message);
      exec_count += 1;
    }
    pipeline.zremrangebyscore(key, "-inf", Date.now());
    exec_count += 1;
    if (maxlen !== null) {
      pipeline.zremrangebyrank(key, 0, -maxlen - 1);
      exec_count += 1;
    }
    return exec_count;
  }

  async function touch(
    pipeline: ChainableCommander,
    {
      pkey,
      value,
    }: {
      pkey: KeyPart;
      value?: Partial<T>;
    },
    { ttl: ttl_in, message }: ValkeyIndexHandlerOptions = {},
  ) {
    let exec_count = 0;
    const key = toKey(pkey);
    const exists = (await valkey.exists(key)) !== 0;
    const relations = value && related ? related(value) : undefined;
    if (ttl_in instanceof Date) {
      pipeline.expireat(key, ttl_in.valueOf());
      exec_count += 1;
    } else if (typeof ttl_in === "number") {
      pipeline.expire(key, ttl_in);
      exec_count += 1;
    } else if (ttl) {
      pipeline.expire(key, ttl);
      exec_count += 1;
    }
    if (message !== undefined) {
      pipeline.publish(key, message);
      exec_count += 1;
    }
    if (relations) {
      if (!exists) {
        for (const [relation, fkey] of Object.entries(relations)) {
          if (Array.isArray(fkey)) {
            fkey.forEach((item) => {
              pipeline.zrem(toKey(item as KeyPart, relation), String(pkey));
              exec_count += 1;
            });
          } else if (fkey !== undefined) {
            pipeline.zrem(toKey(fkey as KeyPart, relation), String(pkey));
            exec_count += 1;
          }
        }
      } else {
        for (const [relation, fkey] of Object.entries(relations)) {
          if (Array.isArray(fkey)) {
            fkey.forEach((item) => {
              exec_count += touchRelated(
                pipeline,
                relation,
                pkey,
                item as KeyPart,
                {
                  ttl: ttl_in,
                  message,
                },
              );
            });
          } else if (fkey !== undefined) {
            exec_count += touchRelated(
              pipeline,
              relation,
              pkey,
              fkey as KeyPart,
              {
                ttl: ttl_in,
                message,
              },
            );
          }
        }
      }
    }
    return exec_count;
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
    }) as AsyncIterableIterator<[string, string]>) {
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
    relation: string;
    signal?: AbortSignal;
    test?: string | RegExp;
  }) {
    if (signal?.aborted) {
      return;
    }
    const key = toKey(fkey, String(relation));
    const subscription = valkey.duplicate();
    await subscription.subscribe(key);
    for await (const [channel, message] of on(subscription, "message", {
      signal,
    }) as AsyncIterableIterator<[string, string]>) {
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
      for (const [relation, fkey] of Object.entries(relations)) {
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

  async function delVia(relation: string, fkey: KeyPart) {
    const key = toKey(fkey, relation);
    const pkeys = await valkey.zrange(key, 0, "-1");
    await valkey.del(key);
    await Promise.all(pkeys.map((pkey) => del(pkey)));
  }

  const ops: ValkeyIndexOps<T, R> = {
    valkey,
    toKey,
    pkeysVia,
    ...{ get, set },
    related,
    touch,
    subscribe,
    subscribeVia,
    del,
    delVia,
  };

  const func = Object.entries(functions).reduce((prev, [key, val]) => {
    prev[key as keyof M] = (async (
      args: Parameters<ValkeyIndexFunction<T, R, M, typeof key>>[1],
      options?: ValkeyIndexHandlerOptions,
    ) => {
      return (val as M[typeof key])(ops, args, options);
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
