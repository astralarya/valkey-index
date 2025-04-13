import type { ChainableCommander } from "iovalkey";

export type ValkeyPipelinerProps = {
  pipeline: ChainableCommander;
};

export function typedTuple<T extends readonly any[] = []>(
  initialElements: T = [] as unknown as T,
) {
  return {
    add<U>(value: U) {
      return typedTuple([...initialElements, value] as const);
    },

    addMany<U extends readonly any[]>(values: U) {
      return typedTuple([...initialElements, ...values] as const);
    },

    build() {
      if (initialElements.length === 0) return [] as never;

      type IsHomogeneous<Tuple extends readonly any[]> =
        Tuple extends readonly [infer First, ...infer Rest]
          ? Rest["length"] extends 0
            ? true
            : First extends Rest[number]
            ? Rest[number] extends First
              ? true
              : false
            : false
          : true;

      return initialElements as unknown as IsHomogeneous<T> extends true
        ? T["length"] extends 0
          ? []
          : T[number][]
        : T;
    },
  };
}

export function typedObject<T extends Record<string, any> = {}>(
  initialObject: T = {} as T,
) {
  return {
    add<K extends string, V>(key: K, value: V) {
      return typedObject({
        ...initialObject,
        [key]: value,
      } as T & { [P in K]: V });
    },
    addMany<U extends Record<string, any>>(obj: U) {
      return typedObject({
        ...initialObject,
        ...obj,
      } as T & U);
    },
    build() {
      return initialObject;
    },
    remove<K extends keyof T>(key: K) {
      const { [key]: _, ...rest } = initialObject;
      return typedObject(rest as Omit<T, K>);
    },
    update<K extends keyof T, V>(key: K, updater: (currentValue: T[K]) => V) {
      return typedObject({
        ...initialObject,
        [key]: updater(initialObject[key]),
      } as Omit<T, K> & { [P in K]: V });
    },
    replace<K extends keyof T, V>(key: K, value: V) {
      return typedObject({
        ...initialObject,
        [key]: value,
      } as Omit<T, K> & { [P in K]: V });
    },
  };
}
