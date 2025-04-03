import SuperJSON from "superjson";

const DEFAULT_FROM_STRING = SuperJSON.parse;
const DEFAULT_TO_STRING = SuperJSON.stringify;

export type ValkeyIndexType<T> = {
  fromString: (input: string) => T;
  fromStringMap: (input: Record<string, string | undefined>) => Partial<T>;
  toString: (input: T) => string;
  toStringMap: (
    input: Partial<T>,
  ) => Record<string, string | undefined> | undefined;
};

export type FromValkey<T> = (input: string) => T & { toString?: () => string };
export type ToValkey<T> = (input: T) => string;

export type FromValkeyMap<T> = { [R in keyof T]: FromValkey<T[R]> };
export type ToValkeyMap<T> = { [R in keyof T]: ToValkey<T[R]> };

export function ValkeyIndexType<T>(
  fromString?: FromValkey<T> | FromValkeyMap<T>,
  toString?: ToValkey<T> | ToValkeyMap<T>,
): ValkeyIndexType<T> {
  return {
    fromString:
      fromString instanceof Function
        ? fromString ?? DEFAULT_FROM_STRING
        : DEFAULT_FROM_STRING,
    fromStringMap: deserializeRecord(
      fromString && !(fromString instanceof Function)
        ? fromString
        : ({} as FromValkeyMap<T>),
    ),
    toString:
      toString instanceof Function
        ? toString ?? DEFAULT_TO_STRING
        : DEFAULT_TO_STRING,
    toStringMap: serializeRecord(
      toString && !(toString instanceof Function)
        ? toString
        : ({} as ToValkeyMap<T>),
    ),
  };
}

function serializeRecord<T>(record: ToValkeyMap<T>) {
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

function deserializeRecord<T>(record: FromValkeyMap<T>) {
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
