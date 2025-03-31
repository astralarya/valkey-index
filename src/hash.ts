import type Redis from "iovalkey";
import {
  DEFAULT_SERIALIZER,
  DEFAULT_DESERIALIZER,
  type ValueSerializer,
  type ValueDeserializer,
} from "./serde";
import {
  type KeyPart,
  validateValkeyName,
  ValkeyIndexer,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
  type ValkeyIndexRef,
  type ValkeyIndexRelations,
} from "./indexer";
import type { ChainableCommander } from "iovalkey";
import { bindHandlers, type ValkeyIndexSpec } from "./handler";

export type ValkeyHashIndexProps<
  T,
  R extends keyof T,
  F extends ValkeyIndexSpec<ValkeyHashIndexInterface<T, R>>,
> = ValkeyIndexerProps<T, R> & {
  exemplar: T | 0;
  relations: R[];
  functions?: F;
} & Partial<ValkeyHashIndexHandlers<T, R>>;

export type ValkeyHashGetter<T, R extends keyof T> = {
  (arg: { pkey: KeyPart; ttl?: Date | number; message?: string }): Promise<
    T | undefined
  >;
  (arg: {
    relation: R;
    fkey: KeyPart;
    ttl?: Date | number;
    message?: string;
  }): Promise<Record<R, T | undefined>>;
};

export type ValkeyHashSetter<T> = (arg: {
  pkey: KeyPart;
  input: T;
  ttl?: Date | number;
  message?: string;
}) => Promise<void>;

export type ValkeyHashUpdater<T> = (arg: {
  pkey: KeyPart;
  input: Partial<T>;
  ttl?: Date | number;
  message?: string;
}) => Promise<void>;

export type ValkeyHashIndexOps<T, R extends keyof T> = {
  get: ValkeyHashGetter<T, R>;
  set: ValkeyHashSetter<T>;
  update: ValkeyHashUpdater<T>;
};

export type ValkeyHashGetHandler<T> = (
  ctx: { valkey: Redis },
  arg: { key: string },
) => Promise<T | undefined>;

export type ValkeyHashSetHandler<T, R extends keyof T> = (
  ctx: ValkeyIndexerReturn<T, R> & {
    pipeline: ChainableCommander;
  },
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyHashUpdateHandler<T, R extends keyof T> = (
  ctx: ValkeyIndexerReturn<T, R> & {
    pipeline: ChainableCommander;
  },
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
  ttl,
  maxlen,
  functions = {} as F,
  relations,
  get: get__,
  set: set__,
  update: update__,
}: ValkeyHashIndexProps<T, R, F>) {
  relations.map((x) => validateValkeyName(String(x)));

  const get_ = get__ || getHash();
  const set_ = set__ || setHash();
  const update_ = update__ || updateHash();

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
    const value = await get_({ valkey }, { key: key({ pkey }) });
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
    ttl: ttl_,
    message,
  }: {
    pkey: KeyPart;
    ttl?: Date | number;
    message?: string;
  }) {
    const value = await get_({ valkey }, { key: key({ pkey }) });
    const pipeline = valkey.multi();
    await touch(pipeline, { pkey, value, ttl: ttl_, message });
    await pipeline.exec();
    return value;
  }

  async function get(arg: {
    pkey: KeyPart;
    ttl?: Date | number;
    message?: string;
  }): Promise<T | undefined>;

  async function get(arg: {
    relation: R;
    fkey: KeyPart;
    ttl?: Date | number;
    message?: string;
  }): Promise<Record<R, T | undefined>>;

  async function get(
    arg: ValkeyIndexRef<T, R> & {
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
              await _get_pkey({ pkey, ttl: arg.ttl, message: arg.message }),
            ] as const;
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

  const ops = {
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

export function getHash<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}) {
  return async function get(
    { valkey }: { valkey: Redis },
    { key }: { key: string },
  ) {
    const value = await valkey.hgetall(key);
    return convert(value);
  };
}

export function setHash<T, R extends keyof T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}) {
  return async function set(
    {
      pipeline,
    }: ValkeyIndexerReturn<T, R> & {
      pipeline: ChainableCommander;
    },
    { key, input }: { key: string; input: T },
  ) {
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

export function updateHash<T, R extends keyof T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<Partial<T>>;
} = {}) {
  return async function update(
    {
      pipeline,
    }: ValkeyIndexerReturn<T, R> & {
      pipeline: ChainableCommander;
    },
    { key, input }: { key: string; input: Partial<T> },
  ) {
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
