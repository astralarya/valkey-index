import { on } from "events";
import type { ChainableCommander } from "iovalkey";
import type Redis from "iovalkey";

export const DEFAULT_TTL = 60 * 60 * 24;
export const DEFAULT_MAXLEN = 8;

export const VALKEY_INDEX_NAME_REGEX = /^[a-zA-Z0-9_./]+$/;

export function validateValkeyName(name: string) {
  if (!VALKEY_INDEX_NAME_REGEX.test(name)) {
    throw TypeError(
      "valkey-index: name may only contain alphanumeric, underscore, and dots",
    );
  }
}

export type KeyPart = string | number | symbol;

export type ValkeyIndexRef<T, R extends keyof T> =
  | {
      pkey: KeyPart;
    }
  | {
      relation: R;
      fkey: KeyPart;
    };

export type ValkeyIndexRelations<T, R extends keyof T> = Record<
  R,
  KeyPart | KeyPart[] | undefined
>;

export type ValkeyIndexEvent<T, R extends keyof T> = {
  source: ValkeyIndexRef<T, R>;
  message: string;
};

export type ValkeyIndexPublish<T, R extends keyof T> =
  | ValkeyIndexRef<T, R>
  | {
      source: ValkeyIndexRef<T, R>;
      channel: ValkeyIndexRef<T, R>;
    };

export type ValkeyIndexerProps<T, R extends keyof T> = {
  valkey: Redis;
  name: string;
  ttl?: number;
  maxlen?: number;
  getRelations?: (ref: {
    pkey: KeyPart;
  }) => ValkeyIndexRelations<T, R> | Promise<ValkeyIndexRelations<T, R>>;
};

export type ValkeyIndexerReturn<T, R extends keyof T> = {
  valkey: Redis;
  name: string;
  ttl?: number;
  maxlen?: number;
  key: (ref: ValkeyIndexRef<T, R>) => string;
  pkeys: (ref: ValkeyIndexRef<T, R>) => Promise<KeyPart[]>;
  mapRelations: (
    ref: {
      pkey: KeyPart;
    },
    func: (ref: ValkeyIndexRef<T, R>) => unknown,
  ) => Promise<void>;
  publish: (
    arg: ValkeyIndexPublish<T, R> & {
      message: string;
    },
  ) => Promise<void>;
  subscribe: (
    arg: ValkeyIndexRef<T, R> & {
      signal?: AbortSignal;
      test?: string | RegExp;
    },
  ) => AsyncGenerator<ValkeyIndexEvent<T, R>>;
  touch: (
    pipeline: ChainableCommander,
    arg: {
      pkey: KeyPart;
      value?: Partial<T>;
      ttl?: Date | number;
      message?: string;
      curr?: ValkeyIndexRelations<T, R>;
      next?: ValkeyIndexRelations<T, R>;
    },
  ) => Promise<void>;
  del: (arg: ValkeyIndexRef<T, R>) => Promise<void>;
};

export type ValkeyIndexerContext<T, R extends keyof T> = ValkeyIndexerReturn<
  T,
  R
> & {
  pipeline: ChainableCommander;
};

export function ValkeyIndexer<T, R extends keyof T>({
  valkey,
  name,
  ttl,
  maxlen,
  getRelations,
}: ValkeyIndexerProps<T, R>): ValkeyIndexerReturn<T, R> {
  validateValkeyName(name);

  function key(ref: ValkeyIndexRef<T, R>) {
    if ("pkey" in ref) {
      return `${name}:${String(ref.pkey)}`;
    } else if ("fkey" in ref && "relation" in ref) {
      return `${name}@${String(ref.relation)}:${String(ref.fkey)}`;
    }
    throw TypeError(
      "valkey-index: toKey() requires a pkey or fkey and relation",
    );
  }

  async function pkeys(ref: ValkeyIndexRef<T, R>) {
    if ("pkey" in ref) {
      return [ref.pkey];
    } else if ("fkey" in ref && "relation" in ref) {
      return valkey.zrange(key(ref), 0, "-1");
    }
    throw TypeError(
      "valkey-index: pkeys() requires a pkey or fkey and relation",
    );
  }

  async function mapRelations(
    ref: { pkey: KeyPart },
    func: (ref: ValkeyIndexRef<T, R>) => unknown,
  ) {
    if (!getRelations) {
      return;
    }
    const relations = await getRelations(ref);
    for (const [relation, fkey] of Object.entries(relations) as [
      R,
      KeyPart | KeyPart[] | undefined,
    ][]) {
      if (Array.isArray(fkey)) {
        fkey.forEach((item) => {
          func({ fkey: item, relation });
        });
      } else if (fkey !== undefined) {
        func({ fkey, relation });
      }
    }
  }

  async function publish(
    arg: ValkeyIndexPublish<T, R> & {
      message: string;
    },
  ) {
    const pipeline = valkey.multi();
    if ("source" in arg && "channel" in arg) {
      const event = stringifyEvent(arg.source, arg.message);
      for (const pkey of await pkeys(arg.channel)) {
        pipeline.publish(key({ pkey }), event);
      }
    } else if ("pkey" in arg) {
      const event = stringifyEvent<T, R>({ pkey: arg.pkey }, arg.message);
      for (const pkey of await pkeys(arg)) {
        pipeline.publish(key({ pkey }), event);
        await mapRelations({ pkey }, (ref) => {
          pipeline.publish(key(ref), event);
        });
      }
    } else if ("fkey" in arg && "relation" in arg) {
      const event = stringifyEvent<T, R>(
        { fkey: arg.fkey, relation: arg.relation },
        arg.message,
      );
      for (const pkey of await pkeys(arg)) {
        pipeline.publish(key({ pkey }), event);
        await mapRelations({ pkey }, (ref) => {
          pipeline.publish(key(ref), event);
        });
      }
    } else {
      throw TypeError(
        "valkey-index: publish() requires a pkey or fkey and relation or source and channel",
      );
    }
    await pipeline.exec();
  }

  async function* subscribe(
    arg: ValkeyIndexRef<T, R> & {
      signal?: AbortSignal;
      test?: string | RegExp;
    },
  ) {
    const { signal, test } = arg;
    if (signal?.aborted) {
      return;
    }
    const key_ = key(arg);
    const subscription = valkey.duplicate();
    await subscription.subscribe(key_);
    for await (const [channel, message] of on(subscription, "message", {
      signal,
    }) as AsyncGenerator<[string, string]>) {
      if (channel === key_) {
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
      ttl?: Date | number;
      message?: string;
    },
  ) {
    const key_ = key({ fkey, relation });
    if (ttl_in instanceof Date) {
      pipeline.zadd(key_, ttl_in.valueOf(), String(pkey));
    } else if (typeof ttl_in === "number") {
      pipeline.zadd(key_, Date.now() + ttl_in, String(pkey));
    } else if (ttl !== undefined) {
      pipeline.zadd(key_, Date.now() + ttl, String(pkey));
    } else {
      pipeline.zadd(key_, "+inf", String(pkey));
    }
    if (ttl !== undefined) {
      pipeline.expire(key_, ttl);
    }
    pipeline.zremrangebyscore(key_, "-inf", Date.now());
    if (maxlen !== undefined) {
      pipeline.zremrangebyrank(key_, 0, -maxlen - 1);
    }
    if (message !== undefined) {
      const event = stringifyEvent({ pkey }, message);
      pipeline.publish(key_, event);
    }
  }

  async function touch(
    pipeline: ChainableCommander,
    {
      pkey,
      ttl: ttl_,
      message,
      curr,
      next,
    }: {
      pkey: KeyPart;
      value?: Partial<T>;
      ttl?: Date | number;
      message?: string;
      curr?: ValkeyIndexRelations<T, R>;
      next?: ValkeyIndexRelations<T, R>;
    },
  ) {
    const key_ = key({ pkey });
    if (ttl_ instanceof Date) {
      pipeline.expireat(key_, ttl_.valueOf());
    } else if (typeof ttl_ === "number") {
      pipeline.expire(key_, ttl_);
    } else if (ttl) {
      pipeline.expire(key_, ttl);
    }
    if (message !== undefined) {
      const event = stringifyEvent({ pkey }, message);
      pipeline.publish(key({ pkey }), event);
    }
    const rm = curr && next && diffRelations({ curr, next });
    if (rm) {
      for (const [relation, fkey] of Object.entries(rm) as [
        R,
        KeyPart | KeyPart[] | undefined,
      ][]) {
        if (Array.isArray(fkey)) {
          fkey.forEach((item) => {
            pipeline.zrem(key({ fkey: item, relation }), String(pkey));
          });
        } else if (fkey !== undefined) {
          pipeline.zrem(key({ fkey, relation }), String(pkey));
        }
      }
    }
    const relations =
      next !== undefined ? next : await getRelations?.({ pkey });
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

  async function del(arg: ValkeyIndexRef<T, R>) {
    const pipeline = valkey.multi();
    if ("pkey" in arg) {
      pipeline.del(key(arg));
      await mapRelations(arg, (ref) => {
        pipeline.zrem(key(ref), String(arg.pkey));
      });
    } else if ("fkey" in arg && "relation" in arg) {
      for (const pkey of await pkeys(arg)) {
        pipeline.del(key({ pkey }));
        await mapRelations({ pkey }, (ref) => {
          pipeline.zrem(key(ref), String(pkey));
        });
      }
      pipeline.del(key(arg));
    }
    pipeline.exec();
  }

  return {
    valkey,
    name,
    ttl,
    maxlen,
    key,
    pkeys,
    mapRelations,
    publish,
    subscribe,
    touch,
    del,
  };
}

export function parseEvent<T, R extends keyof T>(data: string) {
  return JSON.parse(data) as ValkeyIndexEvent<T, R>;
}

export function stringifyEvent<T, R extends keyof T>(
  source: ValkeyIndexRef<T, R>,
  message: string,
) {
  return JSON.stringify({
    source,
    message,
  });
}

export function diffRelations<T, R extends keyof T>({
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
