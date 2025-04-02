import { bindHandlers, type ValkeyIndexSpec } from "./handler";
import {
  ValkeyIndexer,
  type KeyPart,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
} from "./indexer";
import {
  deserializeField,
  serializeField,
  type FieldDeserializer,
  type FieldSerializer,
} from "./serde";

export type ValkeyListIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  exemplar: T | 0;
  functions?: F;
} & Partial<ValkeyListIndexHandlers<T>>;

export type ValkeyListLen = (arg: {
  pkey: KeyPart;
}) => Promise<number | undefined>;

export type ValkeyListPush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<void>;

export type ValkeyListPop<T> = (arg: {
  pkey: KeyPart;
}) => Promise<T | undefined>;

export type ValkeyListRpush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<void>;

export type ValkeyListRpop<T> = (arg: {
  pkey: KeyPart;
}) => Promise<T | undefined>;

export type ValkeyListIndex<T> = (arg: {
  pkey: KeyPart;
  index: number;
}) => Promise<T | null>;

export type ValkeyListIndexOps<T> = {
  len: ValkeyListLen;
  push: ValkeyListPush<T>;
  pop: ValkeyListPop<T>;
  rpush: ValkeyListRpush<T>;
  rpop: ValkeyListRpop<T>;
  index: ValkeyListIndex<T>;
};

export type ValkeyListLenHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<number | undefined>;

export type ValkeyListPushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyListPopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<T | undefined>;

export type ValkeyListRpushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyListRpopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<T | undefined>;

export type ValkeyListIndexHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; index: number },
) => Promise<T | null>;

export type ValkeyListIndexHandlers<T> = {
  len: ValkeyListLenHandler<T>;
  push: ValkeyListPushHandler<T>;
  pop: ValkeyListPopHandler<T>;
  rpush: ValkeyListRpushHandler<T>;
  rpop: ValkeyListRpopHandler<T>;
  index: ValkeyListIndexHandler<T>;
};

export type ValkeyListIndexInterface<T> = ValkeyIndexerReturn<T, never> &
  ValkeyListIndexOps<T>;

export function ValkeyListIndex<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
>({
  valkey,
  name,
  ttl,
  maxlen,
  functions = {} as F,
  len: len__,
  push: push__,
  pop: pop__,
  rpush: rpush__,
  rpop: rpop__,
  index: index__,
}: ValkeyListIndexProps<T, F>) {
  const len_ = len__ || lenList();
  const push_ = push__ || pushList();
  const pop_ = pop__ || popList();
  const rpush_ = rpush__ || rpushList();
  const rpop_ = rpop__ || rpopList();
  const index_ = index__ || indexList();

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

  async function pop({ pkey }: { pkey: KeyPart }) {
    return pop_(indexer, { key: indexer.key({ pkey }) });
  }

  async function rpush({ pkey, input }: { pkey: KeyPart; input: T }) {
    rpush_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function rpop({ pkey }: { pkey: KeyPart }) {
    return rpop_(indexer, { key: indexer.key({ pkey }) });
  }

  async function index({ pkey, index }: { pkey: KeyPart; index: number }) {
    return index_(indexer, { key: indexer.key({ pkey }), index });
  }

  const ops: ValkeyListIndexInterface<T> = {
    ...indexer,
    len,
    push,
    pop,
    rpush,
    rpop,
    index,
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

export function pushList<T>({
  convert = serializeField,
}: {
  convert?: FieldSerializer<T>;
} = {}) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    await valkey.lpush(key, convert(input));
    return;
  };
}

export function popList<T>({
  convert = deserializeField,
}: {
  convert?: FieldDeserializer<T>;
} = {}) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.lpop(key);
    return value ? convert(value) : undefined;
  };
}

export function rpushList<T>({
  convert = serializeField,
}: {
  convert?: FieldSerializer<T>;
} = {}) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, input }: { key: string; input: T },
  ) {
    await valkey.rpush(key, convert(input));
    return;
  };
}

export function rpopList<T>({
  convert = deserializeField,
}: {
  convert?: FieldDeserializer<T>;
} = {}) {
  return async function pop(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key }: { key: string },
  ) {
    const value = await valkey.rpop(key);
    return value ? convert(value) : undefined;
  };
}

export function indexList<T>({
  convert = deserializeField,
}: {
  convert?: FieldDeserializer<T>;
} = {}) {
  return async function index(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { key, index }: { key: string; index: number },
  ) {
    const value = await valkey.lindex(key, index);
    return value ? convert(value) : null;
  };
}
