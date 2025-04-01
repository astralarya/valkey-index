import SuperJSON from "superjson";

export type FieldSerializer<T> = (input: T) => string;

export type FieldDeserializer<T> = (input: string) => T | undefined;

export type RecordSerializer<T> = (
  input: T,
) => Record<string, string | number | undefined> | undefined;

export type RecordDeserializer<T> = (
  input: Record<string, string>,
) => T | undefined;

export function serializeField<T>(input: T) {
  return SuperJSON.stringify(input);
}

export function deserializeField<T>(input: string) {
  return SuperJSON.parse(input) as T | undefined;
}

export function serializeRecordFactory<T>(
  serializer: (x: T[keyof T]) => string,
) {
  return function serialize<T>(input: T) {
    if (!input) {
      return {};
    } else if (typeof input === "object") {
      return Object.fromEntries(
        Object.entries(input).map(([key, val]) => {
          return [key, serializer(val)];
        }),
      );
    }
  };
}

export function deserializeRecordFactory<T>(
  deserializer: (x: string) => T[keyof T],
) {
  return function deserialize<T>(input: Record<string, string | undefined>) {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [key, val ? deserializer(val) : val];
      }),
    ) as T;
  };
}

export function serializeRecord<T>(input: T) {
  if (!input) {
    return {};
  } else if (typeof input === "object") {
    return Object.fromEntries(
      Object.entries(input).map(([key, val]) => {
        return [key, SuperJSON.stringify(val)];
      }),
    );
  }
}

export function deserializeRecord<T>(
  input: Record<string, string | undefined>,
) {
  return Object.fromEntries(
    Object.entries(input).map(([key, val]) => {
      return [key, val ? SuperJSON.parse(val) : val];
    }),
  ) as T;
}
