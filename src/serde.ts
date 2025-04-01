import SuperJSON from "superjson";

export type ValueSerializer<T> = (
  input: T,
) => Record<string, string | number | undefined> | undefined;

export type ValueDeserializer<T> = (
  input: Record<string, string>,
) => T | undefined;

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
