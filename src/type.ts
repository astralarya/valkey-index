import SuperJSON from "superjson";

const DEFAULT_FROM_STRING = SuperJSON.parse;
const DEFAULT_TO_STRING = SuperJSON.stringify;

export type ValkeyType<T> = {
  fromString: (input: string) => T;
  fromStringMap: (input: Record<string, string | undefined>) => Partial<T>;
  toString: (input: T) => string;
  toStringMap: (
    input: Partial<T>,
  ) => Record<string, string | undefined> | undefined;
};

export type FromString<T> = (input: string) => T & { toString?: () => string };
export type ToString<T> = (input: T) => string;

export type FromStringMap<T> = { [R in keyof T]: FromString<T[R]> };
export type ToStringMap<T> = { [R in keyof T]: ToString<T[R]> };

export function ValkeyType<T>(
  fromString?: FromString<T> | FromStringMap<T>,
  toString?: ToString<T> | ToStringMap<T>,
): ValkeyType<T> {
  return {
    fromString:
      fromString instanceof Function
        ? fromString ?? DEFAULT_FROM_STRING
        : DEFAULT_FROM_STRING,
    fromStringMap: deserializeRecord(
      fromString && !(fromString instanceof Function)
        ? fromString
        : ({} as FromStringMap<T>),
    ),
    toString:
      toString instanceof Function
        ? toString ?? DEFAULT_TO_STRING
        : DEFAULT_TO_STRING,
    toStringMap: serializeRecord(
      toString && !(toString instanceof Function)
        ? toString
        : ({} as ToStringMap<T>),
    ),
  };
}

function serializeRecord<T>(record: ToStringMap<T>) {
  return function serialize<T>(input: T) {
    if (!input) {
      return {};
    } else if (typeof input === "object") {
      return Object.fromEntries(
        Object.entries(input).map(([key, val]) => {
          return [
            key,
            (record[key as keyof typeof record] ?? DEFAULT_TO_STRING)(val),
          ];
        }),
      );
    }
  };
}

function deserializeRecord<T>(record: FromStringMap<T>) {
  return function deserialize<T>(input: Record<string, string | undefined>) {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [
          key,
          val
            ? (record[key as keyof typeof record] ?? DEFAULT_FROM_STRING)(val)
            : val,
        ];
      }),
    ) as Partial<T>;
  };
}
