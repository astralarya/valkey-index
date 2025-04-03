import type { ChainableCommander } from "iovalkey";
import { bindHandlers, type ValkeyIndexSpec } from "./handler";
import {
  ValkeyIndexer,
  type KeyPart,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
} from "./indexer";
import {
  ValkeyIndexType,
  type FieldDeserializer,
  type FieldSerializer,
} from "./type";

export type ValkeyListIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  type: ValkeyIndexType<T>;
  functions?: F;
} & Partial<ValkeyListIndexHandlers<T>>;

export type ValkeyListLen = (arg: {
  pkey: KeyPart;
}) => Promise<number | undefined>;

export type ValkeyListPush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<void>;

export type ValkeyListPushx<T> = (arg: {
  pkey: KeyPart;
  input: T[];
}) => Promise<void>;

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
}) => Promise<T | null>;

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

export type ValkeyListLenHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<number | undefined>;

export type ValkeyListPushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyListPushxHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T[] },
) => Promise<void>;

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

export type ValkeyListIndexInterface<T> = ValkeyIndexerReturn<T, never> &
  ValkeyListIndexOps<T>;

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
    push_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function pushx({ pkey, input }: { pkey: KeyPart; input: T[] }) {
    pushx_(indexer, { key: indexer.key({ pkey }), input });
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
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
  };
}

export function lenList<T>() {
  return async function len(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    return await valkey.llen(key);
  };
}

export function pushList<T>(type: ValkeyIndexType<T>) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    await valkey.lpush(key, type.toString(input));
    return;
  };
}

export function pushxList<T>(type: ValkeyIndexType<T>) {
  return async function pushx(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    await valkey.lpushx(key, ...input.map(type.toString));
    return;
  };
}

export function popList<T>(type: ValkeyIndexType<T>) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.lpop(key);
    return value ? type.fromString(value) : undefined;
  };
}

export function rpushList<T>(type: ValkeyIndexType<T>) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    await valkey.rpush(key, type.toString(input));
    return;
  };
}

export function rpushxList<T>(type: ValkeyIndexType<T>) {
  return async function rpushx(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T[] },
  ) {
    await valkey.rpushx(key, ...input.map(type.toString));
    return;
  };
}

export function rpopList<T>(type: ValkeyIndexType<T>) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.rpop(key);
    return value ? type.fromString(value) : undefined;
  };
}

export function indexList<T>(type: ValkeyIndexType<T>) {
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
