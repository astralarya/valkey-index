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

// Example usage:
// Homogeneous - becomes number[]
const homogeneous = typedTuple().add(1).add(2).add(3).build();
// Heterogeneous - becomes readonly [number, string]
const heterogeneous = typedTuple().add(1).add("string").build();
// Many
const many = typedTuple().add(1).addMany([2, 3]).build();
// Complex with addMany
const complex = typedTuple()
  .add(1)
  .addMany([2, 3] as const)
  .add("string")
  .build();
// tricky
const tricky = typedTuple().add(1).add(2).add(3).add("ababa").build();

// With objects
interface User {
  id: number;
  name: string;
}
interface Admin {
  id: number;
  name: string;
  permissions: string[];
}
const user1: User = { id: 1, name: "Alice" };
const user2: User = { id: 2, name: "Bob" };
const admin: Admin = { id: 3, name: "Charlie", permissions: ["all"] };

// Homogeneous objects - becomes User[]
const homogeneousObjects = typedTuple().add(user1).add(user2).build();
// Heterogeneous objects - becomes readonly [User, Admin]
const heterogeneousObjects = typedTuple().add(user1).add(admin).build();
