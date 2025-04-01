import { bindHandlers, type ValkeyIndexSpec } from "./handler";
import {
  ValkeyIndexer,
  type KeyPart,
  type ValkeyIndexerProps,
  type ValkeyIndexerReturn,
} from "./indexer";
import {
  DEFAULT_DESERIALIZER,
  DEFAULT_SERIALIZER,
  type ValueDeserializer,
  type ValueSerializer,
} from "./serde";

export type ValkeyListIndexProps<
  T,
  F extends ValkeyIndexSpec<ValkeyListIndexOps<T>>,
> = ValkeyIndexerProps<T, never> & {
  exemplar: T | 0;
  functions?: F;
} & Partial<ValkeyListIndexHandlers<T>>;

export type ValkeyListPush<T> = (arg: { input: T }) => Promise<void>;

export type ValkeyListPop<T> = () => Promise<T | undefined>;

export type ValkeyListIndexOps<T> = {
  push: ValkeyListPush<T>;
  pop: ValkeyListPop<T>;
};

export type ValkeyListPushHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
  arg: { input: T },
) => Promise<void>;

export type ValkeyListPopHandler<T> = (
  ctx: ValkeyIndexerReturn<T, never>,
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

  async function push({ input }: { input: T }) {
    push_(indexer, { input });
  }

  async function pop() {
    return pop_(indexer);
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
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}) {
  return async function push(
    { valkey }: ValkeyIndexerReturn<T, never>,
    { input }: { input: T },
  ) {
    return;
  };
}

export function popList<T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}) {
  return async function pop(ctx: ValkeyIndexerReturn<T, never>) {
    return undefined;
  };
}
