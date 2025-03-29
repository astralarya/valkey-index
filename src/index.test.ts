import Valkey from "iovalkey";
import {
  createValkeyIndex,
  getHash,
  relatedHash,
  setHash,
  updateHash,
} from ".";

const valkey = new Valkey({
  username: process.env.VKUSERNAME,
  password: process.env.VKPASSWORD,
  host: process.env.VKHOST,
  port: process.env.VKPORT ? parseInt(process.env.VKPORT) : undefined,
});

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
    get: getHash<TestObject>(),
    set: setHash(),
    update: updateHash(),
  },
  {
    use: async ({ valkey, get }, pkey: string, op) => {
      get!(pkey);
      return null;
    },
  },
);

const relationIndex = createValkeyIndex({
  valkey,
  name: "relation",
  related: relatedHash<TestObject, "bar">({ fields: ["bar"] }),
  get: getHash(),
  set: setHash(),
});

test("Hash index", async () => {
  expect(await hashIndex.get("1")).toEqual({});

  await hashIndex.set({ pkey: "1", input: { foo: "ababa", bar: 1 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "ababa", bar: 1 });

  await hashIndex.set({ pkey: "1", input: { foo: "lalala" } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala" });

  await hashIndex.update({ pkey: "1", input: { bar: 2 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 2 });

  await hashIndex.update({ pkey: "1", input: { bar: 4 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 4 });

  await hashIndex.update({ pkey: "1", input: { bar: undefined } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala" });

  await hashIndex.f.use("1");
});

test("Relation index", async () => {
  expect(await relationIndex.get("1")).toEqual({});

  await relationIndex.set({ pkey: "1", input: { foo: "ababa", bar: 2 } });
  expect(await relationIndex.get("1")).toEqual({ foo: "ababa", bar: 2 });
  expect(await relationIndex.pkeysVia("bar", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 4)).toEqual([]);

  await relationIndex.set({ pkey: "1", input: { foo: "lalala" } });
  expect(await relationIndex.get("1")).toEqual({ foo: "lalala" });
  expect(await relationIndex.pkeysVia("bar", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 4)).toEqual([]);

  await relationIndex.set({ pkey: "1", input: { foo: "ababa", bar: 2 } });
  expect(await relationIndex.get("1")).toEqual({ foo: "ababa", bar: 2 });
  expect(await relationIndex.pkeysVia("bar", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("bar", 4)).toEqual([]);

  await relationIndex.set({ pkey: "1", input: { foo: "lalala", bar: 4 } });
  expect(await relationIndex.get("1")).toEqual({ foo: "lalala", bar: 4 });
  expect(await relationIndex.pkeysVia("bar", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("bar", 4)).toEqual(["1"]);
});
