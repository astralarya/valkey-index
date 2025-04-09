import SuperJSON from "superjson";

const DEFAULT_FROM_STRING = SuperJSON.parse;
const DEFAULT_TO_STRING = SuperJSON.stringify;

function DEFAULT_TO_STRING_MAP<T>(input: T) {
  if (!input) {
    return {};
  } else if (typeof input === "object") {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [key, DEFAULT_TO_STRING(val)];
      }),
    );
  }
}

function DEFAULT_FROM_STRING_MAP<T>(input: Record<string, string | undefined>) {
  return Object.fromEntries(
    Object.entries(input).map(([key, val]) => {
      return [key, val ? DEFAULT_FROM_STRING(val) : val];
    }),
  ) as Partial<T>;
}

export type Constructable<T> = ((input: string) => T | undefined) & {
  toString?: () => string;
};

export type FromString<T> = (input: string) => T | undefined;
export type ToString<T> = (input: T) => string;

export type FromStringMap<T> = (
  input: Record<string, string | undefined>,
) => Partial<T> | undefined;
export type ToStringMap<T> = (
  input: T,
) => Record<string, string | undefined> | undefined;

export type ValkeyType<T> = {
  fromString: FromString<T>;
  fromStringMap: FromStringMap<T>;
  toString: ToString<T>;
  toStringMap: ToStringMap<T>;
};

export type ValkeyTypeProps<T> =
  | {
      fromString?: FromString<T>;
      fromStringMap?: FromStringMap<T>;
      toString?: ToString<T>;
      toStringMap?: ToStringMap<T>;
    }
  | Constructable<T>;

export function ValkeyType<T>(props?: ValkeyTypeProps<T>): ValkeyType<T> {
  if (props === undefined) {
    return {
      fromString: DEFAULT_FROM_STRING,
      fromStringMap: DEFAULT_FROM_STRING_MAP,
      toString: DEFAULT_TO_STRING,
      toStringMap: DEFAULT_TO_STRING_MAP,
    };
  } else if (props instanceof Function) {
    return {
      fromString: props,
      fromStringMap: DEFAULT_FROM_STRING_MAP,
      toString: props.toString ? props.toString : DEFAULT_TO_STRING,
      toStringMap: DEFAULT_TO_STRING_MAP,
    };
  } else {
    return {
      fromString: props.fromString ?? DEFAULT_FROM_STRING,
      fromStringMap: props.fromStringMap ?? DEFAULT_FROM_STRING_MAP,
      toString: props.toString ?? DEFAULT_TO_STRING,
      toStringMap: props.toStringMap ?? DEFAULT_TO_STRING_MAP,
    };
  }
}
