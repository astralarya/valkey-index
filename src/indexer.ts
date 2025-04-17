import { on } from "events";
import type { ChainableCommander } from "iovalkey";
import type Redis from "iovalkey";

export const DEFAULT_TTL = 60 * 60 * 24;
export const DEFAULT_MAXLEN = 8;

export const VALKEY_INDEX_NAME_REGEX = /^[a-zA-Z0-9_./]+$/;

export type KeyPart = string | number | symbol;

export type ValkeyIndexRef<T, R extends keyof T> =
  | {
      pkey: KeyPart;
    }
  | {
      relation: R;
      fkey: KeyPart;
    };

export type ValkeyIndexGlobalRef = {
  index: string;
} & (
  | {
      pkey: KeyPart;
    }
  | {
      relation: KeyPart;
      fkey: KeyPart;
    }
);

export type ValkeyIndexRelations<T, R extends keyof T> = Record<R, KeyPart[]>;

export type ValkeyIndexEvent<T> = {
  source: ValkeyIndexGlobalRef;
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
  ) => AsyncGenerator<ValkeyIndexEvent<T>>;
  touch: (
    pipeline: ChainableCommander,
    arg: {
      pkey: KeyPart;
      message?: string;
      ttl?: Date | number;
      prev?: ValkeyIndexRelations<T, R>;
      next?: ValkeyIndexRelations<T, R>;
    },
  ) => void;
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
    throw new ValkeyIndexRefError("ValkeyIndexer:key()");
  }

  async function pkeys(ref: ValkeyIndexRef<T, R>) {
    if ("pkey" in ref) {
      return [ref.pkey];
    } else if ("fkey" in ref && "relation" in ref) {
      return valkey.zrange(key(ref), 0, "-1");
    }
    throw new ValkeyIndexRefError("ValkeyIndexer:pkeys()");
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
      KeyPart[],
    ][]) {
      fkey.forEach((item) => {
        func({ fkey: item, relation });
      });
    }
  }

  async function publish(
    arg: ValkeyIndexPublish<T, R> & {
      message: string;
    },
  ) {
    const pipeline = valkey.multi();
    if ("source" in arg && "channel" in arg) {
      const event = stringifyEvent({ ...arg.source, index: name }, arg.message);
      for (const pkey of await pkeys(arg.channel)) {
        pipeline.publish(key({ pkey }), event);
      }
    } else if ("pkey" in arg) {
      const event = stringifyEvent<T, R>(
        { pkey: arg.pkey, index: name },
        arg.message,
      );
      for (const pkey of await pkeys(arg)) {
        pipeline.publish(key({ pkey }), event);
        await mapRelations({ pkey }, (ref) => {
          pipeline.publish(key(ref), event);
        });
      }
    } else if ("fkey" in arg && "relation" in arg) {
      const event = stringifyEvent<T, R>(
        { fkey: arg.fkey, relation: arg.relation, index: name },
        arg.message,
      );
      for (const pkey of await pkeys(arg)) {
        pipeline.publish(key({ pkey }), event);
        await mapRelations({ pkey }, (ref) => {
          pipeline.publish(key(ref), event);
        });
      }
    } else {
      throw new ValkeyIndexRefError("ValkeyIndexer:publish()");
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
      const event = stringifyEvent({ pkey, index: name }, message);
      pipeline.publish(key_, event);
    }
  }

  function touch(
    pipeline: ChainableCommander,
    {
      pkey,
      ttl: ttl_,
      message,
      prev,
      next,
    }: {
      pkey: KeyPart;
      value?: Partial<T>;
      ttl?: Date | number;
      message?: string;
      prev?: ValkeyIndexRelations<T, R>;
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
      const event = stringifyEvent({ pkey, index: name }, message);
      pipeline.publish(key({ pkey }), event);
    }
    const rm = diffRelations({ prev, next });
    if (rm) {
      for (const [relation, fkey] of Object.entries(rm) as [R, KeyPart[]][]) {
        fkey.forEach((item) => {
          pipeline.zrem(key({ fkey: item, relation }), String(pkey));
        });
      }
    }
    if (next) {
      for (const [relation, fkey] of Object.entries(next) as [R, KeyPart[]][]) {
        fkey.forEach((item) => {
          _touch_related(pipeline, {
            relation,
            pkey,
            fkey: item,
            ttl: ttl_,
            message,
          });
        });
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

export class ValkeyIndexRefError extends Error {
  constructor(source: string, options?: ErrorOptions) {
    const message = `Index references require a pkey or fkey and relation (source '${source}')`;
    super(message, options);
    this.name = "ValkeyIndexRefError";
    this.message = message;
  }
}

export class ValkeyIndexNameError extends Error {
  constructor(input: string, options?: ErrorOptions) {
    const message = `Index and relation names may only use [a-zA-Z0-9_./] (input '${input}')`;
    super(message, options);
    this.name = "ValkeyIndexNameError";
    this.message = message;
  }
}

export class ValkeyIndexRefParseError extends Error {
  constructor(message = "", options?: ErrorOptions) {
    super(message, options);
    this.name = "ValkeyIndexRefParseError";
    this.message = message;
  }
}

export function validateValkeyName(name: string) {
  if (!VALKEY_INDEX_NAME_REGEX.test(name)) {
    throw new ValkeyIndexNameError(name);
  }
}

export function diffRelations<T, R extends keyof T>({
  prev,
  next,
}: {
  prev?: Record<R, KeyPart[]>;
  next?: Record<R, KeyPart[]>;
}) {
  if (prev === undefined || next === undefined) {
    return undefined;
  }
  return Object.fromEntries(
    (Object.entries(prev) as [R, KeyPart[]][]).reduce(
      (accum, [relation, fkeys]: [R, KeyPart[]]) => {
        if (!next || next[relation] === undefined) {
          return accum;
        }
        const newkeys = next[relation];
        accum.push([
          relation,
          fkeys.filter((x) => newkeys.find((y) => x === String(y)) !== -1),
        ]);
        return accum;
      },
      [] as [R, KeyPart[]][],
    ),
  ) as Record<R, KeyPart[]>;
}

function splitOnce(input: string, sep: string) {
  let idx = input.indexOf(sep);
  if (idx === input.length - sep.length) {
    throw new ValkeyIndexRefParseError(`Bad split (sep '${sep}')`);
  }
  if (idx === -1) {
    return [input, ""];
  }
  return [input.slice(0, idx), input.slice(idx + sep.length)] as const;
}

export function parseRef(ref: string): ValkeyIndexGlobalRef {
  try {
    const [path, key] = splitOnce(ref, ":");
    const [index, relation] = splitOnce(path, "@");
    if (!key || !index) {
      throw new ValkeyIndexRefParseError(
        `Failed to parse index ref (parsing '${ref}')`,
      );
    }
    try {
      validateValkeyName(index);
    } catch (err) {
      if (err instanceof ValkeyIndexNameError) {
        throw new ValkeyIndexRefParseError(
          `Failed to parse index ref (parsing '${ref}')`,
          { cause: err },
        );
      } else {
        throw err;
      }
    }
    if (relation) {
      try {
        validateValkeyName(relation);
      } catch (err) {
        if (err instanceof ValkeyIndexNameError) {
          throw new ValkeyIndexRefParseError(
            `Failed to parse index ref (parsing '${ref}')`,
            { cause: err },
          );
        } else {
          throw err;
        }
      }
      return {
        index,
        fkey: key,
        relation,
      };
    } else {
      return {
        index,
        pkey: key,
      };
    }
  } catch (err) {
    if (err instanceof ValkeyIndexRefParseError) {
      throw new ValkeyIndexRefParseError(
        `Failed to parse index ref (parsing '${ref}')`,
        { cause: err },
      );
    } else {
      throw err;
    }
  }
}

export function stringifyRef(ref: ValkeyIndexGlobalRef) {
  if ("pkey" in ref) {
    return `${ref.index}:${String(ref.pkey)}`;
  } else if ("fkey" in ref && "relation" in ref) {
    return `${ref.index}@${String(ref.relation)}:${String(ref.fkey)}`;
  }
}

export function parseEvent<T>(data: string) {
  const { source, message } = JSON.parse(data) as Omit<
    ValkeyIndexEvent<T>,
    "source"
  > & {
    source: string;
  };
  return {
    source: parseRef(source),
    message,
  };
}

export function stringifyEvent<T, R extends keyof T>(
  source: ValkeyIndexGlobalRef,
  message: string,
) {
  return JSON.stringify({
    source: stringifyRef(source),
    message,
  });
}
