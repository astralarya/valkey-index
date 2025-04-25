import type { ChainableCommander } from "iovalkey";
import { bindHandlers, type ValkeyIndexSpec } from "./handler";
import {
  ValkeyIndexer,
  type KeyPart,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
} from "./indexer";
import { ValkeyType } from "./type";
import type { ValkeyPipelineAction, ValkeyPipelineResult } from "./pipeline";

export type ValkeySetAdd<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<number | undefined>;

export type ValkeySetCard = (arg: {
  pkey: KeyPart;
}) => Promise<number | undefined>;

export type ValkeySetIndexOps<T> = {
  add: ValkeySetAdd<T>;
  card: ValkeySetCard;
};

export type ValkeySetAddPipe<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => ValkeyPipelineAction<number | undefined>;

export type ValkeySetCardPipe = (arg: {
  pkey: KeyPart;
}) => ValkeyPipelineAction<number>;

export type ValkeySetIndexPipes<T> = {
  add: ValkeySetAddPipe<T>;
  card: ValkeySetCardPipe;
};

export type ValkeySetIndexInterface<T> = ValkeyIndexerReturn<T, never> &
  ValkeySetIndexOps<T> & {
    pipe: ValkeySetIndexPipes<T>;
  };

export type ValkeySetAddHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<number | undefined>;

export type ValkeySetCardHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<number | undefined>;

export type ValkeySetIndexHandlers<T> = {
  add: ValkeySetAddHandler<T>;
  card: ValkeySetCardHandler<T>;
};

export type ValkeySetAddPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => ValkeyPipelineAction<number | undefined>;

export type ValkeySetCardPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => ValkeyPipelineAction<number>;

export type ValkeySetIndexPipers<T> = {
  add: ValkeySetAddPiper<T>;
  card: ValkeySetCardPiper<T>;
};

export type ValkeySetIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeySetIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  type: ValkeyType<T>;
  functions?: F;
} & Partial<ValkeySetIndexHandlers<T>> & {
    pipe?: Partial<ValkeySetIndexPipers<T>>;
  };

export function ValkeySetIndex<
  T,
  F extends ValkeyIndexSpec<ValkeySetIndexOps<T>>,
>({
  valkey,
  name,
  type,
  ttl,
  maxlen,
  functions = {} as F,
  add: add__,
  card: card__,
  pipe: { add: add_pipe__, card: card_pipe__ } = {},
}: ValkeySetIndexProps<T, F>) {
  const add_ = add__ || addSet(type);
  const card_ = card__ || cardSet();

  const add_pipe_ = add_pipe__ || addSet_pipe(type);
  const card_pipe_ = card_pipe__ || cardSet_pipe();

  const indexer = ValkeyIndexer<T, never>({
    valkey,
    name,
    ttl,
    maxlen,
  });

  async function add({ pkey, input }: { pkey: KeyPart; input: T }) {
    return add_(indexer, { key: indexer.key({ pkey }), input });
  }

  function add_pipe({ pkey, input }: { pkey: KeyPart; input: T }) {
    return add_pipe_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function card({ pkey }: { pkey: KeyPart }) {
    return card_(indexer, { key: indexer.key({ pkey }) });
  }

  function card_pipe({ pkey }: { pkey: KeyPart }) {
    return card_pipe_(indexer, { key: indexer.key({ pkey }) });
  }

  const ops: ValkeySetIndexInterface<T> = {
    ...indexer,
    add,
    card,
    pipe: {
      ...indexer.pipe,
      add: add_pipe,
      card: card_pipe,
    },
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
  };
}

export function addSet<T>(type: ValkeyType<T>) {
  return async function handler(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return valkey.sadd(key, type.toString(input));
  };
}

export function addSet_pipe<T>(type: ValkeyType<T>) {
  return function handler(
    _: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.sadd(key, type.toString(input));
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}

export function cardSet() {
  return async function len(
    { valkey }: ValkeyIndexerReturn<unknown, never>,
    { key }: { key: string },
  ) {
    return await valkey.scard(key);
  };
}

export function cardSet_pipe() {
  return function card(
    _: ValkeyIndexerReturn<unknown, never>,
    { key }: { key: string },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.scard(key);
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}
