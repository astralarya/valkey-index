import type Redis from "iovalkey";
import {
  serializeRecord,
  deserializeRecord,
  ValkeyIndexRecordType,
} from "./type";
import {
  type KeyPart,
  validateValkeyName,
  ValkeyIndexer,
  type ValkeyIndexerContext,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
  type ValkeyIndexRef,
  ValkeyIndexRefError,
  type ValkeyIndexRelations,
} from "./indexer";
import { bindHandlers, type ValkeyIndexSpec } from "./handler";

export type ValkeyHashIndexProps<
  T,
  R extends keyof T,
  F extends ValkeyIndexSpec<ValkeyHashIndexInterface<T, R>>,
> = ValkeyIndexerProps<T, R> & {
  type: ValkeyIndexRecordType<T, keyof T>;
  relations: R[];
  functions?: F;
} & Partial<ValkeyHashIndexHandlers<T, R>>;

export type ValkeyHashGet<T, R extends keyof T> = {
  (arg: {
    pkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<Partial<T> | undefined>;
  (arg: {
    relation: R;
    fkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<Record<R, Partial<T> | undefined>>;
};

export type ValkeyHashSet<T> = (arg: {
  pkey: KeyPart;
  input: T;
  ttl?: Date | number;
  message?: string;
}) => Promise<void>;

export type ValkeyHashUpdate<T> = (arg: {
  pkey: KeyPart;
  input: Partial<T>;
  ttl?: Date | number;
  message?: string;
}) => Promise<void>;

export type ValkeyHashIndexOps<T, R extends keyof T> = {
  get: ValkeyHashGet<T, R>;
  set: ValkeyHashSet<T>;
  update: ValkeyHashUpdate<T>;
};

export type ValkeyHashGetHandler<T> = (
  ctx: { valkey: Redis },
  arg: { key: string; fields?: (keyof T)[] },
) => Promise<Partial<T> | undefined>;

export type ValkeyHashSetHandler<T, R extends keyof T> = (
  ctx: ValkeyIndexerContext<T, R>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyHashUpdateHandler<T, R extends keyof T> = (
  ctx: ValkeyIndexerContext<T, R>,
  arg: { key: string; input: Partial<T> },
) => Promise<void>;

export type ValkeyHashIndexHandlers<T, R extends keyof T> = {
  get: ValkeyHashGetHandler<T>;
  set: ValkeyHashSetHandler<T, R>;
  update: ValkeyHashUpdateHandler<T, R>;
};

export type ValkeyHashIndexInterface<
  T,
  R extends keyof T,
> = ValkeyIndexerReturn<T, R> & ValkeyHashIndexOps<T, R>;

export function ValkeyHashIndex<
  T,
  R extends keyof T,
  F extends ValkeyIndexSpec<ValkeyHashIndexInterface<T, R>>,
>({
  valkey,
  name,
  type,
  ttl,
  maxlen,
  functions = {} as F,
  relations,
  get: get__,
  set: set__,
  update: update__,
}: ValkeyHashIndexProps<T, R, F>) {
  relations.map((x) => validateValkeyName(String(x)));

  const get_ = get__ || getHash(type);
  const set_ = set__ || setHash(type);
  const update_ = update__ || updateHash(type);

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
    return results as ValkeyIndexRelations<T, R>;
  }

  async function getRelations({ pkey }: { pkey: KeyPart }) {
    const value = await get_(
      { valkey },
      { key: key({ pkey }), fields: relations },
    );
    return value ? related(value) : ({} as ValkeyIndexRelations<T, R>);
  }

  const indexer = ValkeyIndexer<T, R>({
    valkey,
    name,
    ttl,
    maxlen,
    getRelations,
  });
  const { key, pkeys, touch } = indexer;

  async function _get_pkey({
    pkey,
    fields,
    ttl: ttl_,
    message,
  }: {
    pkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }) {
    const value = await get_({ valkey }, { key: key({ pkey }), fields });
    const pipeline = valkey.multi();
    await touch(pipeline, { pkey, value, ttl: ttl_, message });
    await pipeline.exec();
    return value;
  }

  async function get(arg: {
    pkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<Partial<T> | undefined>;

  async function get(arg: {
    relation: R;
    fkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<Record<R, Partial<T> | undefined>>;

  async function get(
    arg: ValkeyIndexRef<T, R> & {
      fields?: (keyof T)[];
      ttl?: Date | number;
      message?: string;
    },
  ) {
    if ("pkey" in arg) {
      return _get_pkey(arg);
    } else if ("fkey" in arg && "relation" in arg) {
      return Object.fromEntries(
        await Promise.all(
          (
            await pkeys(arg)
          ).map(async (pkey) => {
            return [
              pkey,
              await _get_pkey({
                pkey,
                fields: arg.fields,
                ttl: arg.ttl,
                message: arg.message,
              }),
            ] as const;
          }),
        ),
      ) as Record<R, T | undefined>;
    }
    throw new ValkeyIndexRefError("ValkeyHashIndex:get()");
  }

  async function set({
    pkey,
    input,
    ttl: ttl_,
    message,
  }: {
    pkey: KeyPart;
    input: T;
    ttl?: Date | number;
    message?: string;
  }) {
    const key_ = key({ pkey });
    const curr_value = await get_({ valkey }, { key: key_ });
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const pipeline = valkey.multi();
    await set_(
      { ...indexer, pipeline },
      {
        key: key_,
        input,
      },
    );
    await touch(pipeline, { pkey, ttl: ttl_, message, curr, next });
    await pipeline.exec();
  }

  async function update({
    pkey,
    input,
    ttl: ttl_,
    message,
  }: {
    pkey: KeyPart;
    input: Partial<T>;
    ttl?: Date | number;
    message?: string;
  }) {
    const key_ = key({ pkey });
    const curr_value = await get_({ valkey }, { key: key_ });
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const pipeline = valkey.multi();
    await update_(
      { ...indexer, pipeline },
      {
        key: key_,
        input,
      },
    );
    await touch(pipeline, { pkey, ttl: ttl_, message, curr, next });
    await pipeline.exec();
    return;
  }

  const ops: ValkeyHashIndexInterface<T, R> = {
    ...indexer,
    get,
    set,
    update,
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
  };
}

export function getHash<T, R extends keyof T>(
  type: ValkeyIndexRecordType<T, keyof T>,
) {
  const fromValkey = deserializeRecord(type);
  return async function get(
    { valkey }: { valkey: Redis },
    { key, fields }: { key: string; fields?: R[] },
  ) {
    if (fields !== undefined) {
      const pipeline = valkey.multi();
      for (const field of fields) {
        pipeline.hget(key, String(field));
      }
      const results = await pipeline.exec();
      const value = Object.fromEntries(
        results?.map(([_, result], idx) => {
          return [fields[idx], result] as const;
        }) ?? [],
      );
      return fromValkey(value);
    } else {
      const value = await valkey.hgetall(key);
      return fromValkey(value);
    }
  };
}

export function setHash<T, R extends keyof T>(
  type: ValkeyIndexRecordType<T, keyof T>,
) {
  const toValkey = serializeRecord(type);
  return async function set(
    { pipeline }: ValkeyIndexerContext<T, R>,
    { key, input }: { key: string; input: T },
  ) {
    if (input === undefined) {
      return;
    }
    const value = toValkey(input);
    if (value === undefined) {
      return;
    }
    pipeline.del(key);
    pipeline.hset(key, value);
  };
}

export function updateHash<T, R extends keyof T>(
  type: ValkeyIndexRecordType<T, keyof T>,
) {
  const toValkey = serializeRecord(type);
  return async function update(
    { pipeline }: ValkeyIndexerContext<T, R>,
    { key, input }: { key: string; input: Partial<T> },
  ) {
    const value = toValkey(input);
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
