import Valkey from "iovalkey";
import {
  createValkeyIndex,
  getHash,
  relatedHash,
  setHash,
  updateHash,
} from ".";

const valkey = new Valkey();

beforeEach(async () => {
  await valkey.flushall();
});

type TestObject = {
  foo: string;
  bar?: number;
};

const hashIndex = createValkeyIndex({
  valkey,
  name: "hash",
  get: getHash<TestObject>(),
  set: setHash<TestObject>(),
  update: updateHash<TestObject>(),
});

const relationIndex = createValkeyIndex({
  valkey,
  name: "relation",
  get: getHash<TestObject>(),
  related: relatedHash({ fields: ["bar"] }),
  set: setHash<TestObject>(),
});

test("Hash index", async () => {
  expect(await hashIndex.get("1")).toEqual({});

  await hashIndex.set({ pkey: "1", input: { foo: "ababa", bar: 1 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "ababa", bar: 1 });

  await hashIndex.set({
    pkey: "1",
    input: { foo: "lalala", bar: undefined /*TODO*/ },
  });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala" });

  await hashIndex.update({ pkey: "1", input: { bar: 2 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 2 });

  await hashIndex.update({ pkey: "1", input: { bar: 4 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 4 });

  await hashIndex.update({ pkey: "1", input: { bar: undefined } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala" });
});

test("Relation index", async () => {
  expect(await relationIndex.get("1")).toEqual({});

  await relationIndex.set({ pkey: "1", input: { foo: "ababa", bar: 2 } });
  expect(await relationIndex.get("1")).toEqual({ foo: "ababa", bar: 2 });

  expect(await relationIndex.pkeysVia("bar", 2)).toEqual(["1"]);

  await relationIndex.set({ pkey: "1", input: { foo: "lalala", bar: 4 } });
  expect(await relationIndex.get("1")).toEqual({ foo: "lalala", bar: 4 });

  expect(await relationIndex.pkeysVia("bar", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 4)).toEqual(["1"]);
});
