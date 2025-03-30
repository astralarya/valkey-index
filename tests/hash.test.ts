import {
  createValkeyIndex,
  getHash,
  setHash,
  updateHash,
  type KeyPart,
} from "../src";
import { useBeforeEach, valkey, type TestObject } from "./index.test";

useBeforeEach();

const hashIndex = createValkeyIndex(
  {
    valkey,
    name: "hash",
    exemplar: 0 as TestObject | 0,
    relations: [],
    get: getHash(),
    set: setHash(),
    update: updateHash(),
  },
  {
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
);

test("Hash", async () => {
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});

  await hashIndex.set({ pkey: 1, input: { foo: "ababa", bar: 0, baz: 1 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 1,
  });

  await hashIndex.set({ pkey: 1, input: { foo: "lalala", bar: 0 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });

  await hashIndex.update({ pkey: 1, input: { baz: 2 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 2,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 4 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });

  await hashIndex.update({ pkey: 1, input: { baz: undefined } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });

  expect(await hashIndex.f.use({ pkey: 1 })).toEqual(undefined);
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });

  await hashIndex.update({ pkey: 1, input: { baz: 0 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 0,
  });

  expect(await hashIndex.f.use({ pkey: 1 })).toEqual(1);
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 1,
  });
});
