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

test("Hash index", async () => {
  await hashIndex.set({ pkey: "1", input: { foo: "ababa", bar: 1 } });
  const value = await hashIndex.get({ pkey: "1" });
  expect(value).toEqual({ foo: "ababa", bar: 1 });
});
