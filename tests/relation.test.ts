import { ValkeyHashIndex, ValkeyIndexType, type KeyPart } from "../src";
import { useBeforeEach, valkey, type TestObject } from "./index.test";

useBeforeEach();

const relationIndex = ValkeyHashIndex({
  valkey,
  name: "relation",
  type: ValkeyIndexType<TestObject>(),
  relations: ["bar", "baz"],
  functions: {
    use: async ({ get, update }, { pkey }: { pkey: KeyPart }) => {
      const val = await get!({ pkey });
      if (val?.baz === undefined) {
        return;
      }
      const next = val.baz + 1;
      await update!({ pkey, input: { baz: next } });
      return next;
    },
  },
});

test("Relations", async () => {
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});

  await relationIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);

  await relationIndex.set({
    pkey: 2,
    input: { foo: "falala", bar: 10, baz: 11 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.set({ pkey: 1, input: { foo: "lalala", bar: 0 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.set({
    pkey: 1,
    input: { foo: "lalala", bar: 0, baz: 4 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  expect(await relationIndex.f.use({ pkey: 1 })).toEqual(5);
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 2, input: { bar: 0 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 0,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 2, input: { bar: 1 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 1, input: { bar: 1 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 1,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "1",
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 1, input: { bar: 0 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.del({ pkey: 1 });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.del({ fkey: 1, relation: "bar" });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);

  await relationIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  await relationIndex.set({
    pkey: 2,
    input: { foo: "lalala", bar: 0, baz: 4 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await relationIndex.get({ fkey: 0, relation: "bar" })).toEqual({
    1: {
      foo: "ababa",
      bar: 0,
      baz: 2,
    },
    2: {
      foo: "lalala",
      bar: 0,
      baz: 4,
    },
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);

  await relationIndex.del({ fkey: 0, relation: "bar" });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);
});
