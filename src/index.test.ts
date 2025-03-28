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
  const get1 = await hashIndex.get("1");
  expect(get1).toEqual({});

  await hashIndex.set({ pkey: "1", input: { foo: "ababa", bar: 1 } });
  const get2 = await hashIndex.get("1");
  expect(get2).toEqual({ foo: "ababa", bar: 1 });

  await hashIndex.update({ pkey: "1", input: { bar: 2 } });
  const get3 = await hashIndex.get("1");
  expect(get3).toEqual({ foo: "ababa", bar: 2 });

  await hashIndex.update({ pkey: "1", input: { bar: undefined } });
  const get4 = await hashIndex.get("1");
  expect(get4).toEqual({ foo: "ababa" });
});

test("Relation index", async () => {
  const get1 = await relationIndex.get("1");
  expect(get1).toEqual({});
  await relationIndex.set({ pkey: "1", input: { foo: "ababa", bar: 2 } });
  const get2 = await relationIndex.get("1");
  expect(get2).toEqual({ foo: "ababa", bar: 2 });

  const pkey1 = await relationIndex.pkeysVia("bar", 2);
  expect(pkey1).toEqual(["1"]);

  await relationIndex.set({ pkey: "1", input: { foo: "lalala", bar: 4 } });
  const get3 = await relationIndex.get("1");
  expect(get3).toEqual({ foo: "lalala", bar: 4 });

  const pkey2 = await relationIndex.pkeysVia("bar", 2);
  expect(pkey2).toEqual([]);
  const pkey3 = await relationIndex.pkeysVia("bar", 4);
  expect(pkey3).toEqual(["1"]);
});
