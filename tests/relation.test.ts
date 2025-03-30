import {
  createValkeyIndex,
  getHash,
  setHash,
  updateHash,
  type KeyPart,
} from "../src";
import { useBeforeEach, valkey, type TestObject } from "./index.test";

useBeforeEach();

const relationIndex = createValkeyIndex(
  {
    valkey,
    name: "relation",
    exemplar: 0 as TestObject | 0,
    relations: ["bar", "baz"],
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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual([]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1", "2"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual(["1", "2"]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);

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
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 1)).toEqual(["2"]);
  expect(await relationIndex.pkeysVia("bar", 10)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 11)).toEqual(["2"]);
});
