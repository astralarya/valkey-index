import Valkey from "iovalkey";
import { createValkeyIndex, getHash, setHash } from ".";

const valkey = new Valkey();

beforeEach(async () => {
  await valkey.flushall();
});

type TestObject = {
  foo: string;
  bar?: number;
};

const hashIndex = createValkeyIndex(
  {
    valkey,
    name: "hash",
  },
  {
    get: getHash<TestObject>(),
    set: setHash<TestObject>(),
  }
);

const relationIndex = createValkeyIndex(
  {
    valkey,
    name: "relation",
    // related: ()
  },
  {
    get: getHash<TestObject>(),
    set: setHash<TestObject>(),
  }
);

test("Hash index", async () => {
  const get1 = await hashIndex.get({ pkey: "1" });
  expect(get1).toEqual({});
  await hashIndex.set({ pkey: "1", input: { foo: "ababa", bar: 1 } });
  const get2 = await hashIndex.get({ pkey: "1" });
  expect(get2).toEqual({ foo: "ababa", bar: 1 });
});
