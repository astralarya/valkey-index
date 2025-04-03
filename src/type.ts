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

export type ValkeyIndexRecordType<T> = {
  [K in keyof T]: ValkeyIndexType<T[K]>;
};

export function ValkeyIndexRecordType<T>() {
  return {} as ValkeyIndexRecordType<T>;
}

export type FieldSerializer<T> = (input: T) => string;

export type FieldDeserializer<T> = (input: string) => T;

export type RecordSerializer<T> = (
  input: T,
) => Record<string, string | number | undefined> | undefined;

export type RecordDeserializer<T> = (input: Record<string, string>) => T;

export function serializeRecord<T>(record: ValkeyIndexRecordType<T>) {
  return function serialize<T>(input: T) {
    if (!input) {
      return {};
    } else if (typeof input === "object") {
      return Object.fromEntries(
        Object.entries(input).map(([key, val]) => {
          return [
            key,
            (
              record[key as keyof typeof record]?.toString ??
              SuperJSON.stringify
            )(val),
          ];
        }),
      );
    }
  };
}

export function deserializeRecord<T>(record: ValkeyIndexRecordType<T>) {
  return function deserialize<T>(input: Record<string, string | undefined>) {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [
          key,
          val
            ? (
                record[key as keyof typeof record]?.fromString ??
                SuperJSON.parse
              )(val)
            : val,
        ];
      }),
    ) as Partial<T>;
  };
}
