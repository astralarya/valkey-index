import type Redis from "iovalkey";
import { type KeyPart } from ".";
import {
  DEFAULT_SERIALIZER,
  DEFAULT_DESERIALIZER,
  type ValueSerializer,
  type ValueDeserializer,
} from "./serde";
import {
  ValkeyIndexer,
  type ValkeyIndexerProps,
  type ValkeyIndexRef,
  type ValkeyIndexRelations,
} from "./indexer";
import type { ChainableCommander } from "iovalkey";
import { bindHandlers, type ValkeyIndexSpec } from "./handler";

export type ValkeyHashIndexProps<
  T,
  R extends keyof T,
  F extends ValkeyIndexSpec<ValkeyHashIndexOps<T, R>>,
> = ValkeyIndexerProps<T, R> & {
  exemplar: T | 0;
  relations: R[];
  functions?: F;
  serializer?: ValueSerializer<T>;
  deserializer?: ValueDeserializer<T>;
  update_serializer?: ValueSerializer<Partial<T>>;
};

export type ValkeyHashIndexOps<T, R extends keyof T> = {
  get: {
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
  set: (arg: {
    pkey: KeyPart;
    input: T;
    ttl?: Date | number;
    message?: string;
  }) => Promise<void>;
  update: (arg: {
    pkey: KeyPart;
    input: Partial<T>;
    ttl?: Date | number;
    message?: string;
  }) => Promise<void>;
};

export function ValkeyHashIndex<
  T,
  R extends keyof T,
  F extends ValkeyIndexSpec<ValkeyHashIndexOps<T, R>>,
>({
  valkey,
  name,
  ttl,
  maxlen,
  functions = {} as F,
  relations,
  serializer = DEFAULT_SERIALIZER,
  deserializer = DEFAULT_DESERIALIZER,
  update_serializer = DEFAULT_SERIALIZER,
}: ValkeyHashIndexProps<T, R, F>) {
  const get_ = getHash({ convert: deserializer });
  const set_ = setHash({ convert: serializer });
  const update_ = updateHash({ convert: update_serializer });

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
    const value = await get_(valkey, { key: key({ pkey }) });
    return value ? related(value) : ({} as ValkeyIndexRelations<T, R>);
  }

  const indexer = ValkeyIndexer<T, R>({
    valkey,
    name,
    ttl,
    maxlen,
    getRelations,
  });
  const { key, pkeys, publish, subscribe, touch, del } = indexer;

  async function _get_pkey({
    pkey,
    ttl: ttl_,
    message,
  }: {
    pkey: KeyPart;
    ttl?: Date | number;
    message?: string;
  }) {
    const value = await get_(valkey, { key: key({ pkey }) });
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
    const curr_value = await get_(valkey, { key: key_ });
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const pipeline = valkey.multi();
    await set_(pipeline, {
      key: key_,
      input,
    });
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
    const curr_value = await get_(valkey, { key: key_ });
    const curr = curr_value ? related(curr_value) : undefined;
    const next = related(input);
    const pipeline = valkey.multi();
    const value = await update_(pipeline, {
      key: key_,
      input,
    });
    await touch(pipeline, { pkey, ttl: ttl_, message, curr, next });
    await pipeline.exec();
    return value;
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
  return async function get(valkey: Redis, { key }: { key: string }) {
    const value = await valkey.hgetall(key);
    return convert(value);
  };
}

export function setHash<T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}) {
  return async function set(
    pipeline: ChainableCommander,
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

export function updateHash<T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<Partial<T>>;
} = {}) {
  return async function update(
    pipeline: ChainableCommander,
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
