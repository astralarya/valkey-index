import {
  type ValkeyIndexGetter,
  type ValkeyIndexSetter,
  type ValkeyIndexUpdater,
} from ".";
import {
  DEFAULT_SERIALIZER,
  DEFAULT_DESERIALIZER,
  type ValueSerializer,
  type ValueDeserializer,
} from "./serde";

export function assembleRecord(fields: string[]) {
  const r: Record<string, string> = {};
  for (let i = 0; i + 2 <= fields.length; i += 2) {
    r[fields[i]!] = fields[i + 1]!;
  }
  return r;
}

export function ValkeyHash<T, R extends keyof T>({
  serializer = DEFAULT_SERIALIZER,
  deserializer = DEFAULT_DESERIALIZER,
  update_serializer = DEFAULT_SERIALIZER,
}: {
  serializer?: ValueSerializer<T>;
  deserializer?: ValueDeserializer<T>;
  update_serializer?: ValueSerializer<Partial<T>>;
}) {
  return {
    get: getHash<T, R>({ convert: deserializer }),
    set: setHash<T, R>({ convert: serializer }),
    update: updateHash<T, R>({ convert: update_serializer }),
  };
}

export function getHash<T, R extends keyof T>({
  convert = DEFAULT_DESERIALIZER,
}: {
  convert?: ValueDeserializer<T>;
} = {}): ValkeyIndexGetter<T, R> {
  return async function get({ valkey }, { key }) {
    const value = await valkey.hgetall(key);
    return convert(value);
  };
}

export function setHash<T, R extends keyof T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<T>;
} = {}): ValkeyIndexSetter<T, R> {
  return async function set(_ops, pipeline, { key, input }) {
    if (input === undefined) {
      return;
    }
    const value = convert(input);
    if (value === undefined) {
      return;
    }
    pipeline.del(key);
    pipeline.hset(key, value);
  };
}

export function updateHash<T, R extends keyof T>({
  convert = DEFAULT_SERIALIZER,
}: {
  convert?: ValueSerializer<Partial<T>>;
} = {}): ValkeyIndexUpdater<T, R> {
  return async function update(_ops, pipeline, { key, input }) {
    const value = convert(input);
    if (value === undefined) {
      return;
    }
    for (const [field, field_value] of Object.entries(value)) {
      if (field_value === undefined) {
        continue;
      }
      pipeline.hset(key, field, field_value);
    }
  };
}
