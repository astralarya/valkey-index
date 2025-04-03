import SuperJSON from "superjson";

export type ValkeyIndexType<T> = {
  fromString: (input: string) => T;
  toString: (input: T) => string;
};

export function ValkeyIndexType<T>(
  fromString?: (input: string) => T & { toString?: () => string },
  toString?: (input: T) => string,
): ValkeyIndexType<T> {
  return {
    fromString: fromString ?? SuperJSON.parse,
    toString: toString ?? fromString?.toString ?? SuperJSON.stringify,
  };
}

export type ValkeyIndexRecordType<T, R extends keyof T> = {
  [X in R]: ValkeyIndexType<T[X]>;
};

export function ValkeyIndexRecordType<T>() {
  return {} as ValkeyIndexRecordType<T, keyof T>;
}

export type FieldSerializer<T> = (input: T) => string;

export type FieldDeserializer<T> = (input: string) => T;

export type RecordSerializer<T> = (
  input: T,
) => Record<string, string | number | undefined> | undefined;

export type RecordDeserializer<T> = (input: Record<string, string>) => T;

export function serializeRecord<T, R extends keyof T>(
  record: ValkeyIndexRecordType<T, R>,
) {
  return function serialize<T>(input: T) {
    if (!input) {
      return {};
    } else if (typeof input === "object") {
      return Object.fromEntries(
        Object.entries(input).map(([key, val]) => {
          return [
            key,
            (record[key as R]?.toString ?? SuperJSON.stringify)(val),
          ];
        }),
      );
    }
  };
}

export function deserializeRecord<T, R extends keyof T>(
  record: ValkeyIndexRecordType<T, R>,
) {
  return function deserialize<T>(input: Record<string, string | undefined>) {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [
          key,
          val ? (record[key as R]?.fromString ?? SuperJSON.parse)(val) : val,
        ];
      }),
    ) as Partial<T>;
  };
}
