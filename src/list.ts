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
}) => Promise<T | null | undefined>;

export type ValkeyListRpush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<number>;

export type ValkeyListRpushx<T> = (arg: {
  pkey: KeyPart;
  input: T[];
}) => Promise<number>;

export type ValkeyListRpop<T> = (arg: {
  pkey: KeyPart;
}) => Promise<T | null | undefined>;

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

export type ValkeyListPopPipe<T> = (arg: {
  pkey: KeyPart;
}) => ValkeyPipelineAction<T | null | undefined>;

export type ValkeyListRpushPipe<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => ValkeyPipelineAction<void>;

export type ValkeyListRpushxPipe<T> = (arg: {
  pkey: KeyPart;
  input: T[];
}) => ValkeyPipelineAction<void>;

export type ValkeyListRpopPipe<T> = (arg: {
  pkey: KeyPart;
}) => ValkeyPipelineAction<T | null | undefined>;

export type ValkeyListIndexPipe<T> = (arg: {
  pkey: KeyPart;
  index: number;
}) => ValkeyPipelineAction<T | null | undefined>;

export type ValkeyListTrimPipe<T> = (arg: {
  pkey: KeyPart;
  start: number;
  stop: number;
  pipeline?: ChainableCommander;
}) => ValkeyPipelineAction<void>;

export type ValkeyListIndexPipes<T> = {
  len: ValkeyListLenPipe;
  push: ValkeyListPushPipe<T>;
  pushx: ValkeyListPushxPipe<T>;
  pop: ValkeyListPopPipe<T>;
  rpush: ValkeyListRpushPipe<T>;
  rpushx: ValkeyListRpushxPipe<T>;
  rpop: ValkeyListRpopPipe<T>;
  index: ValkeyListIndexPipe<T>;
  trim: ValkeyListTrimPipe<T>;
};

export type ValkeyListIndexInterface<T> = ValkeyIndexerReturn<T, never> &
  ValkeyListIndexOps<T> & {
    pipe: ValkeyListIndexPipes<T>;
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
) => Promise<T | null>;

export type ValkeyListRpushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<number>;

export type ValkeyListRpushxHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T[] },
) => Promise<number>;

export type ValkeyListRpopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<T | null>;

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

export type ValkeyListPopPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => ValkeyPipelineAction<T | null>;

export type ValkeyListRpushPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => ValkeyPipelineAction<void>;

export type ValkeyListRpushxPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T[] },
) => ValkeyPipelineAction<void>;

export type ValkeyListRpopPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => ValkeyPipelineAction<T | null>;

export type ValkeyListIndexPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; index: number },
) => ValkeyPipelineAction<T | null>;

export type ValkeyListTrimPiper<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: {
    key: string;
    start: number;
    stop: number;
    pipeline?: ChainableCommander;
  },
) => ValkeyPipelineAction<void>;

export type ValkeyListIndexPipers<T> = {
  len: ValkeyListLenPiper<T>;
  push: ValkeyListPushPiper<T>;
  pushx: ValkeyListPushxPiper<T>;
  pop: ValkeyListPopPiper<T>;
  rpush: ValkeyListRpushPiper<T>;
  rpushx: ValkeyListRpushxPiper<T>;
  rpop: ValkeyListRpopPiper<T>;
  index: ValkeyListIndexPiper<T>;
  trim: ValkeyListTrimPiper<T>;
};

export type ValkeyListIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  type: ValkeyType<T>;
  functions?: F;
} & Partial<ValkeyListIndexHandlers<T>> & {
    pipe?: Partial<ValkeyListIndexPipers<T>>;
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
  pipe: {
    len: len_pipe__,
    push: push_pipe__,
    pushx: pushx_pipe__,
    pop: pop_pipe__,
    rpush: rpush_pipe__,
    rpushx: rpushx_pipe__,
    rpop: rpop_pipe__,
    index: index_pipe__,
    trim: trim_pipe__,
  } = {},
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

  const len_pipe_ = len_pipe__ || lenList_pipe();
  const push_pipe_ = push_pipe__ || pushList_pipe(type);
  const pushx_pipe_ = pushx_pipe__ || pushxList_pipe(type);
  const pop_pipe_ = pop_pipe__ || popList_pipe(type);
  const rpush_pipe_ = rpush_pipe__ || rpushList_pipe(type);
  const rpushx_pipe_ = rpushx_pipe__ || rpushxList_pipe(type);
  const rpop_pipe_ = rpop_pipe__ || rpopList_pipe(type);
  const index_pipe_ = index_pipe__ || indexList_pipe(type);
  const trim_pipe_ = trim_pipe__ || trimList_pipe();

  const indexer = ValkeyIndexer<T, never>({
    valkey,
    name,
    ttl,
    maxlen,
  });

  async function len({ pkey }: { pkey: KeyPart }) {
    return len_(indexer, { key: indexer.key({ pkey }) });
  }

  function len_pipe({ pkey }: { pkey: KeyPart }) {
    return len_pipe_(indexer, { key: indexer.key({ pkey }) });
  }

  async function push({ pkey, input }: { pkey: KeyPart; input: T }) {
    return push_(indexer, { key: indexer.key({ pkey }), input });
  }

  function push_pipe({ pkey, input }: { pkey: KeyPart; input: T }) {
    return push_pipe_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function pushx({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    return pushx_(indexer, { key: indexer.key({ pkey }), input });
  }

  function pushx_pipe({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    return pushx_pipe_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function pop({ pkey }: { pkey: KeyPart }) {
    return pop_(indexer, { key: indexer.key({ pkey }) });
  }

  function pop_pipe({ pkey }: { pkey: KeyPart }) {
    return pop_pipe_(indexer, { key: indexer.key({ pkey }) });
  }

  async function rpush({ pkey, input }: { pkey: KeyPart; input: T }) {
    return rpush_(indexer, { key: indexer.key({ pkey }), input });
  }

  function rpush_pipe({ pkey, input }: { pkey: KeyPart; input: T }) {
    return rpush_pipe_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function rpushx({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    return rpushx_(indexer, { key: indexer.key({ pkey }), input });
  }

  function rpushx_pipe({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    return rpushx_pipe_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function rpop({ pkey }: { pkey: KeyPart }) {
    return rpop_(indexer, { key: indexer.key({ pkey }) });
  }

  function rpop_pipe({ pkey }: { pkey: KeyPart }) {
    return rpop_pipe_(indexer, { key: indexer.key({ pkey }) });
  }

  async function index({ pkey, index }: { pkey: KeyPart; index: number }) {
    return index_(indexer, { key: indexer.key({ pkey }), index });
  }

  function index_pipe({ pkey, index }: { pkey: KeyPart; index: number }) {
    return index_pipe_(indexer, { key: indexer.key({ pkey }), index });
  }

  async function trim({
    pkey,
    start,
    stop,
  }: {
    pkey: KeyPart;
    start: number;
    stop: number;
  }) {
    return trim_(indexer, {
      key: indexer.key({ pkey }),
      start,
      stop,
    });
  }

  function trim_pipe({
    pkey,
    start,
    stop,
  }: {
    pkey: KeyPart;
    start: number;
    stop: number;
  }) {
    return trim_pipe_(indexer, {
      key: indexer.key({ pkey }),
      start,
      stop,
    });
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
      len: len_pipe,
      push: push_pipe,
      pushx: pushx_pipe,
      pop: pop_pipe,
      rpush: rpush_pipe,
      rpushx: rpushx_pipe,
      rpop: rpop_pipe,
      index: index_pipe,
      trim: trim_pipe,
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

export function pushList<T>(type: ValkeyType<T>) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return valkey.lpush(key, type.toString(input));
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

export function pushxList<T>(type: ValkeyType<T>) {
  return async function pushx(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    return valkey.lpushx(key, ...input.map(type.toString));
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

export function popList<T>(type: ValkeyType<T>) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.lpop(key);
    return value === null ? null : type.fromString(value);
  };
}

export function popList_pipe<T>(type: ValkeyType<T>) {
  return function pop(
    _: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.lpop(key);
      return function getter(results: ValkeyPipelineResult) {
        const value = results[idx]?.[1] as string;
        return value === null ? null : type.fromString(value);
      };
    };
  };
}

export function rpushList<T>(type: ValkeyType<T>) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return await valkey.rpush(key, type.toString(input));
  };
}

export function rpushList_pipe<T>(type: ValkeyType<T>) {
  return function push(
    _: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.rpush(key, type.toString(input));
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}

export function rpushxList<T>(type: ValkeyType<T>) {
  return async function rpushx(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    return await valkey.rpushx(key, ...input.map(type.toString));
  };
}

export function rpushxList_pipe<T>(type: ValkeyType<T>) {
  return function rpushx(
    _: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.rpushx(key, ...input.map(type.toString));
      return function getter(results: ValkeyPipelineResult) {
        return results[idx]?.[1] as number;
      };
    };
  };
}

export function rpopList<T>(type: ValkeyType<T>) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.rpop(key);
    return value === null ? null : type.fromString(value);
  };
}

export function rpopList_pipe<T>(type: ValkeyType<T>) {
  return function pop(
    _: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.rpop(key);
      return function getter(results: ValkeyPipelineResult) {
        const value = results[idx]?.[1] as string;
        return value === null ? null : type.fromString(value);
      };
    };
  };
}

export function indexList<T>(type: ValkeyType<T>) {
  return async function index(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, index }: { key: string; index: number },
  ) {
    const value = await valkey.lindex(key, index);
    return value === null ? null : type.fromString(value);
  };
}

export function indexList_pipe<T>(type: ValkeyType<T>) {
  return function index(
    _: ValkeyIndexerReturn<T, never>,
    { key, index }: { key: string; index: number },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.lindex(key, index);
      return function getter(results: ValkeyPipelineResult) {
        const value = results[idx]?.[1] as string;
        return value === null ? null : type.fromString(value);
      };
    };
  };
}

export function trimList<T>() {
  return async function index(
    { valkey }: ValkeyIndexerReturn<T, never>,
    {
      key,
      start,
      stop,
    }: {
      key: string;
      start: number;
      stop: number;
    },
  ) {
    await valkey.ltrim(key, start, stop);
  };
}

export function trimList_pipe<T>() {
  return function index(
    _: ValkeyIndexerReturn<T, never>,
    {
      key,
      start,
      stop,
    }: {
      key: string;
      start: number;
      stop: number;
    },
  ) {
    return function pipe(pipeline: ChainableCommander) {
      const idx = pipeline.length;
      pipeline.ltrim(key, start, stop);
      return function getter(results: ValkeyPipelineResult) {
        return;
      };
    };
  };
}
