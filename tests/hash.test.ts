import { ValkeyHashIndex, ValkeyType, type KeyPart } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";

useBeforeEach();

const hashIndex = ValkeyHashIndex({
  valkey,
  name: "hash",
  type: ValkeyType<TestObject>(),
  relations: [],
  functions: {
    use: async ({ get, update }, { pkey }: { pkey: KeyPart }) => {
      const val = await get({ pkey });
      if (val?.baz === undefined) {
        return;
      }
      const next = val.baz + 1;
      await update({ pkey, input: { baz: next } });
      return next;
    },
  },
});

test("Hash", async () => {
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});
  expect(await hashIndex.get({ pkey: 2 })).toEqual({});

  await hashIndex.set({ pkey: 1, input: { foo: "ababa", bar: 0, baz: 1 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 1, fields: ["foo", "bar"] })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await hashIndex.get({ pkey: 1, fields: ["baz", "bar"] })).toEqual({
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({});

  await hashIndex.set({ pkey: 2, input: { foo: "falala", bar: 10, baz: 11 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.set({ pkey: 1, input: { foo: "lalala", bar: 0 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 2 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 2,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 4 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: undefined } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  expect(await hashIndex.f.use({ pkey: 1 })).toEqual(undefined);
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 0 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 0,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  expect(await hashIndex.f.use({ pkey: 1 })).toEqual(1);
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.del({ pkey: 1 });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.del({ pkey: 2 });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});
  expect(await hashIndex.get({ pkey: 2 })).toEqual({});
});
