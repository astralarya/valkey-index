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

export type ValkeyListIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  type: ValkeyType<T>;
  functions?: F;
} & Partial<ValkeyListIndexHandlers<T>> & {
    pipe?: Partial<ValkeyListIndexPipers<T>>;
  };

export type ValkeyListLen = (arg: {
  pkey: KeyPart;
}) => Promise<number | undefined>;

export type ValkeyListPush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<number>;

export type ValkeyListPushx<T> = (arg: {
  pkey: KeyPart;
  input: T[];
}) => Promise<number>;

export type ValkeyListPop<T> = (arg: {
  pkey: KeyPart;
}) => Promise<T | undefined>;

export type ValkeyListRpush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<void>;

export type ValkeyListRpushx<T> = (arg: {
  pkey: KeyPart;
  input: T[];
}) => Promise<void>;

export type ValkeyListRpop<T> = (arg: {
  pkey: KeyPart;
}) => Promise<T | undefined>;

export type ValkeyListIndex<T> = (arg: {
  pkey: KeyPart;
  index: number;
}) => Promise<T | null | undefined>;

export type ValkeyListTrim<T> = (arg: {
  pkey: KeyPart;
  start: number;
  stop: number;
  pipeline?: ChainableCommander;
}) => Promise<void>;

export type ValkeyListIndexOps<T> = {
  len: ValkeyListLen;
  push: ValkeyListPush<T>;
  pushx: ValkeyListPushx<T>;
  pop: ValkeyListPop<T>;
  rpush: ValkeyListRpush<T>;
  rpushx: ValkeyListRpushx<T>;
  rpop: ValkeyListRpop<T>;
  index: ValkeyListIndex<T>;
  trim: ValkeyListTrim<T>;
};

export type ValkeyListLenPipe = (arg: {
  pkey: KeyPart;
}) => ValkeyPipelineAction<number>;

export type ValkeyListPushPipe<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => ValkeyPipelineAction<number>;

export type ValkeyListPushxPipe<T> = (arg: {
  pkey: KeyPart;
  input: T[];
}) => ValkeyPipelineAction<number>;

export type ValkeyListIndexPipes<T> = {
  len: ValkeyListLenPipe;
  push: ValkeyListPushPipe<T>;
  pushx: ValkeyListPushxPipe<T>;
};

export type ValkeyListLenHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<number | undefined>;

export type ValkeyListPushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<number>;

export type ValkeyListPushxHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T[] },
) => Promise<number>;

export type ValkeyListPopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<T | undefined>;

export type ValkeyListRpushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyListRpushxHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T[] },
) => Promise<void>;

export type ValkeyListRpopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<T | undefined>;

export type ValkeyListIndexHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; index: number },
) => Promise<T | null>;

export type ValkeyListTrimHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: {
    key: string;
    start: number;
    stop: number;
    pipeline?: ChainableCommander;
  },
) => Promise<void>;

export type ValkeyListIndexHandlers<T> = {
  len: ValkeyListLenHandler<T>;
  push: ValkeyListPushHandler<T>;
  pushx: ValkeyListPushxHandler<T>;
  pop: ValkeyListPopHandler<T>;
  rpush: ValkeyListRpushHandler<T>;
  rpushx: ValkeyListRpushxHandler<T>;
  rpop: ValkeyListRpopHandler<T>;
  index: ValkeyListIndexHandler<T>;
  trim: ValkeyListTrimHandler<T>;
};

export type ValkeyListLenPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => ValkeyPipelineAction<number>;

export type ValkeyListPushPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => ValkeyPipelineAction<number>;

export type ValkeyListPushxPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T[] },
) => ValkeyPipelineAction<number>;

export type ValkeyListIndexPipers<T> = {
  len: ValkeyListLenPiper<T>;
  push: ValkeyListPushPiper<T>;
  pushx: ValkeyListPushxPiper<T>;
};

export type ValkeyListIndexInterface<T> = ValkeyIndexerReturn<T, never> &
  ValkeyListIndexOps<T> & {
    pipe: ValkeyListIndexPipes<T>;
  };

export function ValkeyListIndex<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
>({
  valkey,
  name,
  type,
  ttl,
  maxlen,
  functions = {} as F,
  len: len__,
  push: push__,
  pushx: pushx__,
  pop: pop__,
  rpush: rpush__,
  rpushx: rpushx__,
  rpop: rpop__,
  index: index__,
  trim: trim__,
  pipe: { len: len_pipe__, push: push_pipe__, pushx: pushx_pipe__ } = {},
}: ValkeyListIndexProps<T, F>) {
  const len_ = len__ || lenList();
  const push_ = push__ || pushList(type);
  const pushx_ = pushx__ || pushxList(type);
  const pop_ = pop__ || popList(type);
  const rpush_ = rpush__ || rpushList(type);
  const rpushx_ = rpushx__ || rpushxList(type);
  const rpop_ = rpop__ || rpopList(type);
  const index_ = index__ || indexList(type);
  const trim_ = trim__ || trimList();

  const pipe_len_ = len_pipe__ || lenList_pipe();
  const pipe_push_ = push_pipe__ || pushList_pipe(type);
  const pipe_pushx_ = pushx_pipe__ || pushxList_pipe(type);

  const indexer = ValkeyIndexer<T, never>({
    valkey,
    name,
    ttl,
    maxlen,
  });

  async function len({ pkey }: { pkey: KeyPart }) {
    return len_(indexer, { key: indexer.key({ pkey }) });
  }

  async function push({ pkey, input }: { pkey: KeyPart; input: T }) {
    return push_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function pushx({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    return pushx_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function pop({ pkey }: { pkey: KeyPart }) {
    return pop_(indexer, { key: indexer.key({ pkey }) });
  }

  async function rpush({ pkey, input }: { pkey: KeyPart; input: T }) {
    rpush_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function rpushx({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    rpushx_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function rpop({ pkey }: { pkey: KeyPart }) {
    return rpop_(indexer, { key: indexer.key({ pkey }) });
  }

  async function index({ pkey, index }: { pkey: KeyPart; index: number }) {
    return index_(indexer, { key: indexer.key({ pkey }), index });
  }

  async function trim({
    pkey,
    start,
    stop,
    pipeline,
  }: {
    pkey: KeyPart;
    start: number;
    stop: number;
    pipeline?: ChainableCommander;
  }) {
    return trim_(indexer, {
      key: indexer.key({ pkey }),
      start,
      stop,
      pipeline,
    });
  }

  function pipe_len({ pkey }: { pkey: KeyPart }) {
    return pipe_len_(indexer, { key: indexer.key({ pkey }) });
  }

  function pipe_push({ pkey, input }: { pkey: KeyPart; input: T }) {
    return pipe_push_(indexer, { key: indexer.key({ pkey }), input });
  }

  function pipe_pushx({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    return pipe_pushx_(indexer, { key: indexer.key({ pkey }), input });
  }

  const ops: ValkeyListIndexInterface<T> = {
    ...indexer,
    len,
    push,
    pushx,
    pop,
    rpush,
    rpushx,
    rpop,
    index,
    trim,
    pipe: {
      len: pipe_len,
      push: pipe_push,
      pushx: pipe_pushx,
    },
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
  };
}

export function lenList() {
  return async function len(
    { valkey }: ValkeyIndexerReturn<unknown, never>,
    { key }: { key: string },
  ) {
    return await valkey.llen(key);
  };
}

export function pushList<T>(type: ValkeyType<T>) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return valkey.lpush(key, type.toString(input));
  };
}

export function pushxList<T>(type: ValkeyType<T>) {
  return async function pushx(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    return valkey.lpushx(key, ...input.map(type.toString));
  };
}

export function popList<T>(type: ValkeyType<T>) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.lpop(key);
    return value ? type.fromString(value) : undefined;
  };
}

export function rpushList<T>(type: ValkeyType<T>) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    await valkey.rpush(key, type.toString(input));
    return;
  };
}

export function rpushxList<T>(type: ValkeyType<T>) {
  return async function rpushx(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    await valkey.rpushx(key, ...input.map(type.toString));
    return;
  };
}

export function rpopList<T>(type: ValkeyType<T>) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.rpop(key);
    return value ? type.fromString(value) : undefined;
  };
}

export function indexList<T>(type: ValkeyType<T>) {
  return async function index(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, index }: { key: string; index: number },
  ) {
    const value = await valkey.lindex(key, index);
    return value ? type.fromString(value) : null;
  };
}

export function trimList<T>() {
  return async function index(
    { valkey }: ValkeyIndexerReturn<T, never>,
    {
      key,
      start,
      stop,
      pipeline,
    }: {
      key: string;
      start: number;
      stop: number;
      pipeline?: ChainableCommander;
    },
  ) {
    if (pipeline) {
      pipeline.ltrim(key, start, stop);
    } else {
      await valkey.ltrim(key, start, stop);
    }
  };
}

export function lenList_pipe() {
  return function len(
    _: ValkeyIndexerReturn<unknown, never>,
    { key }: { key: string },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.llen(key);
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}

export function pushList_pipe<T>(type: ValkeyType<T>) {
  return function push(
    _: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.lpush(key, type.toString(input));
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}

export function pushxList_pipe<T>(type: ValkeyType<T>) {
  return function pushx(
    _: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.lpushx(key, ...input.map(type.toString));
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}
