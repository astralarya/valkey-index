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

export type ValkeyListPush<T> = (arg: {
  pkey: KeyPart;
  input: T;
}) => Promise<void>;

export type ValkeyListPop<T> = (arg: {
  pkey: KeyPart;
}) => Promise<T | undefined>;

export type ValkeyListIndexOps<T> = {
  push: ValkeyListPush<T>;
  pop: ValkeyListPop<T>;
};

export type ValkeyListPushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string; input: T },
) => Promise<void>;

export type ValkeyListPopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { key: string },
) => Promise<T | undefined>;

export type ValkeyListIndexHandlers<T> = {
  push: ValkeyListPushHandler<T>;
  pop: ValkeyListPopHandler<T>;
};

export type ValkeyListIndexInterface<T> = ValkeyIndexerReturn<T, never> &
  ValkeyListIndexOps<T>;

export function ValkeyStreamIndex<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
>({
  valkey,
  name,
  ttl,
  maxlen,
  functions = {} as F,
  push: push__,
  pop: pop__,
}: ValkeyListIndexProps<T, F>) {
  const push_ = push__ || pushList();
  const pop_ = pop__ || popList();

  const indexer = ValkeyIndexer<T, never>({
    valkey,
    name,
    ttl,
    maxlen,
  });

  async function push({ pkey, input }: { pkey: KeyPart; input: T }) {
    push_(indexer, { key: indexer.key({ pkey }), input });
  }

  async function pop({ pkey }: { pkey: KeyPart }) {
    return pop_(indexer, { key: indexer.key({ pkey }) });
  }

  const ops: ValkeyListIndexInterface<T> = {
    ...indexer,
    push,
    pop,
  };

  return {
    ...ops,
    f: bindHandlers(ops, functions),
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
