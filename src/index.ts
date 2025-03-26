import { on } from "events";
import type Redis from "iovalkey";
import { type ChainableCommander } from "iovalkey";

export * from "./handler";

export const DEFAULT_TTL = 60 * 60 * 24;
export const DEFAULT_MAXLEN = 8;

export type KeyPart = string | number | symbol;

export type ValkeyIndex<R extends KeyPart> = {
  valkey: Redis;
  // name/relation limited to alphanum, underscore, dot
  name: string;
  related?: (
    pkey: KeyPart,
  ) => Promise<Record<R, KeyPart[] | KeyPart | undefined>>;
  ttl?: number | null;
  maxlen?: number | null;
};

export type ValkeyIndexHandlerOptions = {
  ttl?: Date | number;
  message?: string;
};

export type ValkeyIndexOps<R extends KeyPart> = {
  valkey: Redis;
  ttl?: number | null;
  maxlen?: number | null;
  toKey: (id: KeyPart, relation?: string) => string;
  pkeysVia: (relation: string, fkey: KeyPart) => Promise<string[]>;
  related:
    | ((pkey: KeyPart) => Promise<Record<R, KeyPart[] | KeyPart | undefined>>)
    | undefined;
  touch: (
    pipeline: ChainableCommander,
    pkey: KeyPart,
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
export type ValkeyIndexHandler<A> = (
  ops: ValkeyIndexOps<KeyPart>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<void>;
export type ValkeyIndexItemHandler<A, T> = (
  ops: ValkeyIndexOps<KeyPart>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<T>;
export type ValkeyIndexStreamHandler<A, T> = (
  ops: ValkeyIndexOps<KeyPart>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => Promise<StreamItem<T | undefined>[]>;
export type ValkeyIndexSubscriptionHandler<A, T> = (
  ops: ValkeyIndexOps<KeyPart>,
  arg: A,
  options?: ValkeyIndexHandlerOptions,
) => AsyncGenerator<StreamItem<T | undefined>>;

export type ValkeyIndexSpec<T> = Record<
  string,
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexHandler<any>
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexItemHandler<any, T>
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexStreamHandler<any, T>
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  | ValkeyIndexSubscriptionHandler<any, T>
>;

export type ValkeyIndexFunction<
  T,
  M extends ValkeyIndexSpec<T>,
  K extends keyof M,
> =
  M[K] extends ValkeyIndexHandler<infer A>
    ? (arg: A, options?: ValkeyIndexHandlerOptions) => Promise<void>
    : M[K] extends ValkeyIndexItemHandler<infer A, infer R>
      ? (arg: A, options?: ValkeyIndexHandlerOptions) => Promise<R>
      : M[K] extends ValkeyIndexStreamHandler<infer A, infer R>
        ? (
            arg: A,
            options?: ValkeyIndexHandlerOptions,
          ) => Promise<StreamItem<R | undefined>[]>
        : M[K] extends ValkeyIndexSubscriptionHandler<infer A, infer R>
          ? (
              arg: A,
              options?: ValkeyIndexHandlerOptions,
            ) => Promise<AsyncGenerator<StreamItem<R | undefined>>>
          : never;

export type ValkeyIndexInterface<T, M extends ValkeyIndexSpec<T>> = {
  [K in keyof M]: ValkeyIndexFunction<T, M, K>;
};

export type StreamItem<T> = {
  id: string;
  data: T;
};

export function createValkeyIndex<
  T,
  R extends KeyPart,
  M extends ValkeyIndexSpec<T>,
>(
  {
    valkey,
    name,
    related,
    ttl = DEFAULT_TTL,
    maxlen = DEFAULT_MAXLEN,
  }: ValkeyIndex<R>,
  functions = {} as M,
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
    pkey: KeyPart,
    { ttl: ttl_in, message }: ValkeyIndexHandlerOptions = {},
  ) {
    let exec_count = 0;
    const key = toKey(pkey);
    const exists = (await valkey.exists(key)) !== 0;
    const relations = related ? await related(key) : null;
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
    const relations = related ? await related(key) : null;
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

  const ops: ValkeyIndexOps<R> = {
    valkey,
    toKey,
    pkeysVia,
    related,
    touch,
    subscribe,
    subscribeVia,
    del,
    delVia,
  };

  const func = Object.entries(functions).reduce(
    (prev, [key, val]) => {
      prev[key as keyof M] = (async (...args) => {
        return val(ops, ...args);
      }) as ValkeyIndexFunction<T, M, typeof key>;
      return prev;
    },
    {} as ValkeyIndexInterface<T, M>,
  );

  return {
    ...ops,
    func,
    f: func,
    get: func.get as ValkeyIndexFunction<T, M, "get">,
    set: func.set as ValkeyIndexFunction<T, M, "set">,
    append: func.append as ValkeyIndexFunction<T, M, "append">,
    range: func.range as ValkeyIndexFunction<T, M, "range">,
    read: func.read as ValkeyIndexFunction<T, M, "read">,
  };
}
