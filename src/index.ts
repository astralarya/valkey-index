import { on } from "events";
import type Redis from "iovalkey";
import { type ChainableCommander } from "iovalkey";

export * from "./handler";

export const DEFAULT_TTL = 60 * 60 * 24;
export const DEFAULT_MAXLEN = 8;

export const VALKEY_INDEX_NAME_REGEX = /^[a-zA-Z0-9_./]+$/;

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

export type ValkeyIndexRef<T, R extends keyof T> =
  | {
      pkey: KeyPart;
    }
  | {
      relation: R;
      fkey: KeyPart;
    };

export type ValkeyIndexToucherOptions = {
  ttl?: Date | number;
  message?: string;
};

export type ValkeyIndexGetter<T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  arg: { key: string } & ValkeyIndexToucherOptions,
) => Promise<T | undefined>;

export type ValkeyIndexGetHandler<T, R extends keyof T> = <
  Ref extends ValkeyIndexRef<T, R>,
>(
  arg: Ref & ValkeyIndexToucherOptions,
) => Ref extends { pkey: KeyPart }
  ? Promise<T | undefined>
  : Ref extends { fkey: KeyPart; relation: R }
  ? Promise<Record<R, T | undefined>>
  : never;

export type ValkeyIndexSetter<T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  pipeline: ChainableCommander,
  arg: { key: string; input: T } & ValkeyIndexToucherOptions,
) => Promise<void> | void;

export type ValkeyIndexUpdater<T, R extends keyof T> = (
  ops: ValkeyIndexOps<T, R>,
  pipeline: ChainableCommander,
  arg: { key: string; input: Partial<T> } & ValkeyIndexToucherOptions,
) => Promise<void> | void;

export type ValkeyIndexOps<T, R extends keyof T> = {
  valkey: Redis;
  name: string;
  relations: R[];
  ttl?: number | null;
  maxlen?: number | null;
  key: (ref: ValkeyIndexRef<T, R>) => string;
  pkeys: (ref: ValkeyIndexRef<T, R>) => Promise<string[]>;
  related: (
    value: Partial<T>,
  ) => Record<R, KeyPart[] | KeyPart | undefined> | undefined;
  get?: ValkeyIndexGetHandler<T, R>;
  set?: (
    arg: { pkey: KeyPart; input: T } & ValkeyIndexToucherOptions,
  ) => Promise<void>;
  update?: (
    arg: { pkey: KeyPart; input: Partial<T> } & ValkeyIndexToucherOptions,
  ) => Promise<void>;
  touch: (
    pipeline: ChainableCommander,
    arg: {
      pkey: KeyPart;
      value?: Partial<T>;
    } & ValkeyIndexToucherOptions,
  ) => Promise<void>;
  publish: (
    arg: ValkeyIndexRef<T, R> & {
      message: string;
    },
  ) => Promise<void>;
  subscribe: (
    x: ValkeyIndexRef<T, R> & {
      signal?: AbortSignal | undefined;
      test?: string | RegExp;
    },
  ) => AsyncGenerator<ValkeyIndexEvent<T, R>>;
  del: (arg: ValkeyIndexRef<T, R>) => Promise<void>;
};

export type ValkeyIndexCommand<A, T> = (arg: A) => Promise<T>;
export type ValkeyIndexHandler<A, T, R extends keyof T, V> = (
  ops: ValkeyIndexOps<T, R>,
  arg: A,
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
  ? (arg: A) => V
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

export type ValkeyIndexEvent<T, R extends keyof T> = {
  source: ValkeyIndexRef<T, R>;
  message: string;
};

function parseEvent<T, R extends keyof T>(data: string) {
  return JSON.parse(data) as ValkeyIndexEvent<T, R>;
}

function stringifyEvent<T, R extends keyof T>(
  source: ValkeyIndexRef<T, R>,
  message: string,
) {
  return JSON.stringify({
    source,
    message,
  });
}

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
    : ValkeyIndexGetHandler<T, R>;
  set: (typeof index)["set"] extends undefined
    ? undefined
    : (
        arg: { pkey: KeyPart; input: T } & ValkeyIndexToucherOptions,
      ) => Promise<void>;
  update: (typeof index)["update"] extends undefined
    ? undefined
    : (
        arg: { pkey: KeyPart; input: Partial<T> } & ValkeyIndexToucherOptions,
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
    get: get_arg,
    set: set_,
    update: update_,
    ttl = DEFAULT_TTL,
    maxlen = DEFAULT_MAXLEN,
  }: ValkeyIndexOptions<T, R>,
  functions = {} as M,
) {
  if (!VALKEY_INDEX_NAME_REGEX.test(name)) {
    throw TypeError(
      "valkey-index: name may only contain alphanumeric, underscore, and dots",
    );
  }
  if (relations.find((x) => !VALKEY_INDEX_NAME_REGEX.test(String(x)))) {
    throw TypeError(
      "valkey-index: related names may only contain alphanumeric, underscore, and dots",
    );
  }

  function _key(ref: ValkeyIndexRef<T, R>) {
    if ("pkey" in ref) {
      return `${name}:${String(ref.pkey)}`;
    } else if ("fkey" in ref && "relation" in ref) {
      return `${name}@${String(ref.relation)}:${String(ref.fkey)}`;
    }
    throw TypeError(
      "valkey-index: toKey() requires a pkey or fkey and relation",
    );
  }

  async function _pkeys(ref: ValkeyIndexRef<T, R>) {
    if ("pkey" in ref) {
      return [_key(ref)];
    } else if ("fkey" in ref && "relation" in ref) {
      return valkey.zrange(_key(ref), 0, "-1");
    }
    throw TypeError(
      "valkey-index: pkeys() requires a pkey or fkey and relation",
    );
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

  async function _get_pkey({
    pkey,
    ttl: ttl_,
    message,
  }: { pkey: KeyPart } & ValkeyIndexToucherOptions) {
    if (!get_arg) {
      throw TypeError("valkey-index: get() invoked without being defined");
    }
    const key = _key({ pkey });
    const value = await get_arg(ops, { key });
    const pipeline = valkey.multi();
    await touch(pipeline, { pkey, value, ttl: ttl_, message });
    await pipeline.exec();
    return value;
  }

  async function get(arg: ValkeyIndexRef<T, R> & ValkeyIndexToucherOptions) {
    if ("pkey" in arg) {
      return _get_pkey(arg);
    } else if ("fkey" in arg && "relation" in arg) {
      const pkeys = await _pkeys(arg);
      if (!pkeys) {
        return {} as Record<R, T | undefined>;
      }
      return Object.fromEntries(
        await Promise.all(
          pkeys.map(async (pkey) => {
            return [pkey, await _get_pkey({ ...arg, pkey })] as const;
          }),
        ),
      ) as Record<R, T | undefined>;
    }
    throw TypeError("valkey-index: get() requires a pkey or relation and fkey");
  }

  async function set({
    pkey,
    input,
    ttl: ttl_,
    message,
  }: { pkey: KeyPart; input: T } & ValkeyIndexToucherOptions) {
    if (!get_arg) {
      throw TypeError(
        "valkey-index: set() invoked without get() being defined",
      );
    }
    if (!set_) {
      throw TypeError("valkey-index: set() invoked without being defined");
    }
    const key = _key({ pkey });
    const curr_value = await get_arg(ops, { key });
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const rm = calculateRm<T, R>({ curr, next });
    const pipeline = valkey.multi();
    const value = await set_(ops, pipeline, { key, input, ttl: ttl_, message });
    await touch(pipeline, { pkey, value: input, rm, ttl: ttl_, message });
    await pipeline.exec();
    return value;
  }

  async function update({
    pkey,
    input,
    ttl: ttl_,
    message,
  }: { pkey: KeyPart; input: Partial<T> } & ValkeyIndexToucherOptions) {
    if (!get_arg) {
      throw TypeError(
        "valkey-index: update() invoked without get() being defined",
      );
    }
    if (!update_) {
      throw TypeError("valkey-index: update() invoked without being defined");
    }
    const key = _key({ pkey });
    const curr_value = await get_arg(ops, { key, ttl: ttl_, message });
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const rm = calculateRm<T, R>({ curr, next });
    const pipeline = valkey.multi();
    const value = await update_!(ops, pipeline, {
      key,
      input,
      ttl: ttl_,
      message,
    });
    await touch(pipeline, { pkey, value: input, rm, ttl: ttl_, message });
    await pipeline.exec();
    return value;
  }

  function _touch_related(
    pipeline: ChainableCommander,
    {
      pkey,
      fkey,
      relation,
      ttl: ttl_in,
      message,
    }: {
      relation: R;
      pkey: KeyPart;
      fkey: KeyPart;
    } & ValkeyIndexToucherOptions,
  ) {
    const key = _key({ fkey, relation });
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
      const event = stringifyEvent({ pkey }, message);
      pipeline.publish(key, event);
    }
  }

  async function touch(
    pipeline: ChainableCommander,
    {
      pkey,
      value,
      rm,
      ttl: ttl_,
      message,
    }: {
      pkey: KeyPart;
      value?: Partial<T>;
      rm?: Record<R, KeyPart[] | KeyPart | undefined>;
    } & ValkeyIndexToucherOptions,
  ) {
    const key = _key({ pkey });
    // const exists = (await valkey.exists(key)) !== 0;
    const relations = value && related ? related(value) : undefined;
    if (ttl_ instanceof Date) {
      pipeline.expireat(key, ttl_.valueOf());
    } else if (typeof ttl_ === "number") {
      pipeline.expire(key, ttl_);
    } else if (ttl) {
      pipeline.expire(key, ttl);
    }
    if (message !== undefined) {
      const event = stringifyEvent({ pkey }, message);
      pipeline.publish(key, event);
    }
    if (rm) {
      for (const [relation, fkey] of Object.entries(rm) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            pipeline.zrem(_key({ fkey: item, relation }), String(pkey));
          });
        } else if (fkey !== undefined) {
          pipeline.zrem(_key({ fkey, relation }), String(pkey));
        }
      }
    }
    if (relations) {
      for (const [relation, fkey] of Object.entries(relations) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            _touch_related(pipeline, {
              relation,
              pkey,
              fkey: item,
              ttl: ttl_,
              message,
            });
          });
        } else if (fkey !== undefined) {
          _touch_related(pipeline, {
            relation,
            pkey,
            fkey,
            ttl: ttl_,
            message,
          });
        }
      }
    }
  }

  async function _publish_pkey(
    pipeline: ChainableCommander,
    {
      pkey,
      message,
    }: {
      pkey: KeyPart;
      message: string;
    },
  ) {
    const value = await _get_pkey({ pkey });
    const relations = value && related ? related(value) : undefined;
    const event = stringifyEvent({ pkey }, message);
    pipeline.publish(_key({ pkey }), event);
    if (relations) {
      for (const [relation, fkey] of Object.entries(relations) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            pipeline.publish(_key({ fkey: item, relation }), event);
          });
        } else if (fkey !== undefined) {
          pipeline.publish(_key({ fkey, relation }), event);
        }
      }
    }
  }

  async function publish(arg: ValkeyIndexRef<T, R> & { message: string }) {
    const pipeline = valkey.multi();
    if ("pkey" in arg) {
      await _publish_pkey(pipeline, arg);
    } else if ("fkey" in arg && "relation" in arg) {
      const pkeys = await _pkeys(arg);
      if (!pkeys) {
        return;
      }
      for (const pkey of pkeys) {
        await _publish_pkey(pipeline, { pkey, message: arg.message });
      }
    } else {
      throw TypeError(
        "valkey-index: publish() requires a pkey or relation and fkey",
      );
    }
    await pipeline.exec();
  }

  async function* _subscribe_pkey({
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
    const key = _key({ pkey });
    const subscription = valkey.duplicate();
    await subscription.subscribe(key);
    for await (const [channel, message] of on(subscription, "message", {
      signal,
    }) as AsyncGenerator<[string, string]>) {
      if (channel === key) {
        const event = parseEvent(message);
        if (test === undefined) {
          yield event;
        } else if (test instanceof RegExp) {
          if (event?.message && test.test(event.message)) {
            yield event;
          }
        } else if (event?.message === test) {
          yield event;
        }
      }
    }
  }

  async function* _subscribe_fkey({
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
    const key = _key({ fkey, relation });
    const subscription = valkey.duplicate();
    await subscription.subscribe(key);
    for await (const [channel, message] of on(subscription, "message", {
      signal,
    }) as AsyncGenerator<[string, string]>) {
      if (channel === key) {
        const event = parseEvent(message);
        if (test === undefined) {
          yield event;
        } else if (test instanceof RegExp) {
          if (event?.message && test.test(event.message)) {
            yield event;
          }
        } else if (event?.message === test) {
          yield event;
        }
      }
    }
  }

  // HOPE bun fixes https://github.com/oven-sh/bun/issues/17591
  // until then this leaks connections
  function subscribe(
    arg: ValkeyIndexRef<T, R> & {
      signal?: AbortSignal;
      test?: string | RegExp;
    },
  ) {
    if ("pkey" in arg) {
      return _subscribe_pkey(arg);
    } else if ("fkey" in arg && "relation" in arg) {
      return _subscribe_fkey(arg);
    }
    throw TypeError(
      "valkey-index: subscribe() requires a pkey or relation and fkey",
    );
  }

  async function _del_pkey({ pkey }: { pkey: KeyPart }) {
    const key = _key({ pkey });
    const value = _get_pkey ? await _get_pkey({ pkey }) : undefined;
    const relations = value && related ? related(value) : null;
    const pipeline = valkey.multi();
    if (relations) {
      for (const [relation, fkey] of Object.entries(relations) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            pipeline.zrem(_key({ fkey: item, relation }), String(pkey));
          });
        } else if (fkey !== undefined) {
          pipeline.zrem(_key({ fkey, relation }), String(pkey));
        }
      }
    }
    pipeline.del(key);
    await pipeline.exec();
  }

  async function del(arg: ValkeyIndexRef<T, R>) {
    if ("pkey" in arg) {
      return _del_pkey(arg);
    } else if ("fkey" in arg && "relation" in arg) {
      const key = _key(arg);
      const pkeys = await valkey.zrange(key, 0, "-1");
      await valkey.del(key);
      await Promise.all(pkeys.map((pkey) => _del_pkey({ pkey })));
    }
  }

  const ops: ValkeyIndexOps<T, R> = {
    valkey,
    name,
    relations,
    key: _key,
    pkeys: _pkeys,
    ...{ get: get as ValkeyIndexGetHandler<T, R>, set, update },
    related,
    touch,
    publish,
    subscribe,
    del,
  };

  const func = Object.entries(functions).reduce((prev, [key, val]) => {
    prev[key as keyof M] = ((
      arg: Parameters<ValkeyIndexFunction<T, R, M, typeof key>>[1],
    ) => {
      return val(ops, arg);
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
