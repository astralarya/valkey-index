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
      type NewT = K extends keyof T
        ? Omit<T, K> &
            Record<
              K,
              T[K] extends readonly any[]
                ? T[K][number] extends V
                  ? V extends T[K][number]
                    ? V[]
                    : [...T[K], V]
                  : [...T[K], V]
                : T[K] extends V
                ? V extends T[K]
                  ? V[]
                  : readonly [T[K], V]
                : readonly [T[K], V]
            >
        : T & Record<K, [V]>;

      const newObject = { ...initialObject } as NewT;

      if (key in initialObject) {
        const existingValue = initialObject[key as keyof T];

        (newObject as any)[key] = [...existingValue, value];
      } else {
        (newObject as any)[key] = [value];
      }

      return typedObject(newObject);
    },

    build() {
      return initialObject as T;
    },
  };
}
