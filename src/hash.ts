import type Redis from "iovalkey";
import { ValkeyType } from "./type";
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
import type { ChainableCommander } from "iovalkey";
import type { ValkeyPipelineAction, ValkeyPipelineResult } from "./pipeline";

export type ValkeyHashGet<T, R extends keyof T> = {
  (arg: {
    pkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<T | undefined>;
  (arg: {
    relation: R;
    fkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<Record<R, T | undefined>>;
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

export type ValkeyHashReduce<T> = (arg: {
  pkey: KeyPart;
  reducer: (prev: T | undefined) => T | undefined;
  ttl?: Date | number;
  message?: string;
}) => Promise<T | undefined>;

export type ValkeyHashIndexOps<T, R extends keyof T> = {
  get: ValkeyHashGet<T, R>;
  set: ValkeyHashSet<T>;
  update: ValkeyHashUpdate<T>;
  reduce: ValkeyHashReduce<T>;
};

export type ValkeyHashGetPipe<T, R extends keyof T> = {
  (arg: {
    pkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): ValkeyPipelineAction<T | undefined>;
  (arg: {
    relation: R;
    fkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): ValkeyPipelineAction<Record<R, T | undefined>>;
};

export type ValkeyHashSetPipe<T> = (arg: {
  pkey: KeyPart;
  input: T;
  ttl?: Date | number;
  message?: string;
}) => ValkeyPipelineAction<void>;

export type ValkeyHashIndexPipes<T, R extends keyof T> = {
  get: ValkeyHashGetPipe<T, R>;
  set: ValkeyHashSetPipe<T>;
};

export type ValkeyHashIndexInterface<
  T,
  R extends keyof T,
> = ValkeyIndexerReturn<T, R> & ValkeyHashIndexOps<T, R> /*& {
    pipe: ValkeyHashIndexPipes<T, R>;
  }*/;

export type ValkeyHashGetHandler<T> = (
  ctx: { valkey: Redis },
  arg: { key: string; fields?: (keyof T)[] },
) => Promise<T | undefined>;

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

export type ValkeyHashGetPiper<T> = (
  ctx: { valkey: Redis },
  arg: { key: string; fields?: (keyof T)[] },
) => Promise<T | undefined>;

export type ValkeyHashSetPiper<T, R extends keyof T> = (
  ctx: ValkeyIndexerContext<T, R>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyHashIndexPipers<T, R extends keyof T> = {
  get: ValkeyHashGetPiper<T>;
  set: ValkeyHashSetPiper<T, R>;
};

export type ValkeyHashIndexProps<
  T,
  R extends keyof T,
  F extends ValkeyIndexSpec<ValkeyHashIndexInterface<T, R>>,
> = ValkeyIndexerProps<T, R> & {
  type: ValkeyType<T>;
  relations: R[];
  functions?: F;
} & Partial<ValkeyHashIndexHandlers<T, R>> & {
    pipe?: Partial<ValkeyHashIndexPipers<T, R>>;
  };

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
  pipe: { get: get_pipe__, set: set_pipe__ } = {},
}: ValkeyHashIndexProps<T, R, F>) {
  relations.map((x) => validateValkeyName(String(x)));

  const get_ =
    get__ ||
    async function (_, { key, fields }: { key: string; fields?: (keyof T)[] }) {
      const pipe = getHash(type)({ key, fields });
      const pipeline = valkey.pipeline();
      const getter = pipe(pipeline);
      const results = await pipeline.exec();
      if (!results) {
        return undefined;
      }
      return getter(results);
    };
  const set_ =
    set__ ||
    async function (
      { pipeline }: { pipeline: ChainableCommander },
      { key, input }: { key: string; input: T },
    ) {
      const pipe = setHash(type)({ key, input });
      pipe(pipeline);
      await pipeline.exec();
    };
  const update_ = update__ || updateHash(type);

  const get_pipe_ = get_pipe__ || getHash(type);
  const set_pipe_ = set_pipe__ || setHash(type);

  function related(value: Partial<T>) {
    return Object.fromEntries(
      relations.map((relation) => {
        const fval = value[relation];
        if (Array.isArray(fval)) {
          return [relation, fval.map((x) => String(x))];
        } else if (fval === undefined || fval === null) {
          return [relation, []];
        }
        return [relation, [String(fval)]];
      }),
    ) as ValkeyIndexRelations<T, R>;
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
  const { key, pkeys, touch, del } = indexer;

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
    const curr = value ? related(value) : undefined;
    const pipeline = valkey.multi();
    touch(pipeline, { pkey, message, ttl: ttl_, curr });
    await pipeline.exec();
    return value;
  }

  async function get(arg: {
    pkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<T | undefined>;

  async function get(arg: {
    relation: R;
    fkey: KeyPart;
    fields?: (keyof T)[];
    ttl?: Date | number;
    message?: string;
  }): Promise<Record<R, T | undefined>>;

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
      ) as Record<R, Partial<T> | undefined>;
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
    touch(pipeline, { pkey, message, ttl: ttl_, curr, next });
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
    const next = related({ ...curr, ...input });
    const pipeline = valkey.multi();
    await update_(
      { ...indexer, pipeline },
      {
        key: key_,
        input,
      },
    );
    touch(pipeline, { pkey, message, ttl: ttl_, curr, next });
    await pipeline.exec();
    return;
  }

  async function reduce({
    pkey,
    reducer,
    ttl: ttl_,
    message,
  }: {
    pkey: KeyPart;
    reducer: (prev: T | undefined) => T | undefined;
    ttl?: Date | number;
    message?: string;
  }) {
    const key_ = key({ pkey });
    const curr_value = await get_({ valkey }, { key: key_ });
    const curr = curr_value ? related(curr_value) : undefined;
    const next_value = reducer(curr_value);
    const next = next_value ? related(next_value) : undefined;
    const pipeline = valkey.multi();
    if (next_value === undefined) {
      if (curr_value !== undefined) {
        await del({ pkey });
      }
    } else {
      await update_(
        { ...indexer, pipeline },
        {
          key: key_,
          input: next_value,
        },
      );
      touch(pipeline, { pkey, message, ttl: ttl_, curr, next });
    }
    await pipeline.exec();
    return next_value;
  }

  const ops: ValkeyHashIndexInterface<T, R> = {
    ...indexer,
    get,
    set,
    update,
    reduce,
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
  };
}

export function getHash<T>(type: ValkeyType<T>) {
  return function get({ key, fields }: { key: string; fields?: (keyof T)[] }) {
    return function pipe(pipeline: ChainableCommander) {
      if (fields !== undefined) {
        const idx = pipeline.length;
        for (const field of fields) {
          pipeline.hget(key, String(field));
        }
        const idx_end = pipeline.length;
        return function getter(results: ValkeyPipelineResult) {
          const value = Object.fromEntries(
            results?.slice(idx, idx_end).map(([_, result], idx) => {
              return [fields[idx], result] as const;
            }) ?? [],
          );
          return type.fromStringMap(value);
        };
      } else {
        const idx = pipeline.length;
        pipeline.hgetall(key);
        return function getter(results: ValkeyPipelineResult) {
          return type.fromStringMap(
            results[idx]?.[1] as Record<string, string>,
          );
        };
      }
    };
  };
}

export function setHash<T>(type: ValkeyType<T>) {
  return function set({ key, input }: { key: string; input: T }) {
    return function pipe(pipeline: ChainableCommander) {
      if (input === undefined) {
        return;
      }
      const value = type.toStringMap(input);
      if (value === undefined) {
        return;
      }
      pipeline.del(key);
      pipeline.hset(key, value);
    };
  };
}

export function updateHash<T, R extends keyof T>(type: ValkeyType<T>) {
  return async function update(
    { pipeline }: ValkeyIndexerContext<T, R>,
    { key, input }: { key: string; input: Partial<T> },
  ) {
    const value = type.toStringMap(input);
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
