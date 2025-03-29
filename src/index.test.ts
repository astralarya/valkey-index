import Valkey from "iovalkey";
import {
  appendStream,
  createValkeyIndex,
  getHash,
  rangeStream,
  readStream,
  setHash,
  updateHash,
  type Exemplar,
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
  bar: number;
  baz?: number;
};

test("Sanity checks", () => {
  const bad_names = ["bad:name", "bad@name", "bad/name", "bad-name"] as const;

  bad_names.forEach((bad_name) => {
    expect(() => {
      createValkeyIndex({
        valkey,
        name: bad_name,
        exemplar: undefined,
        relations: [],
      });
    }).toThrow(Error);
  });

  bad_names.forEach((bad_name, idx) => {
    expect(() => {
      createValkeyIndex({
        valkey,
        name: "good_name",
        exemplar: 0 as Exemplar<Record<(typeof bad_names)[typeof idx], 0>>,
        relations: [bad_name],
      });
    }).toThrow(Error);
  });

  expect(() => {
    createValkeyIndex({
      name: "good_name",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name.property",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name.property",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name.property",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);
});

const hashIndex = createValkeyIndex(
  {
    valkey,
    name: "hash",
    exemplar: 0 as Exemplar<TestObject>,
    relations: [],
    get: getHash(),
    set: setHash(),
    update: updateHash(),
  },
  {
    use: async ({ get, update }, pkey: string, op) => {
      const val = await get!(pkey);
      if (val?.baz === undefined) {
        return;
      }
      const next = val.baz + 1;
      await update!({ pkey, input: { baz: next } });
      return next;
    },
  },
);

test("Hash index", async () => {
  expect(await hashIndex.get("1")).toEqual({});

  await hashIndex.set({ pkey: "1", input: { foo: "ababa", bar: 0, baz: 1 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "ababa", bar: 0, baz: 1 });

  await hashIndex.set({ pkey: "1", input: { foo: "lalala", bar: 0 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0 });

  await hashIndex.update({ pkey: "1", input: { baz: 2 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0, baz: 2 });

  await hashIndex.update({ pkey: "1", input: { baz: 4 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0, baz: 4 });

  await hashIndex.update({ pkey: "1", input: { baz: undefined } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0 });

  expect(await hashIndex.f.use("1")).toEqual(undefined);
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0 });

  await hashIndex.update({ pkey: "1", input: { baz: 0 } });
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0, baz: 0 });

  expect(await hashIndex.f.use("1")).toEqual(1);
  expect(await hashIndex.get("1")).toEqual({ foo: "lalala", bar: 0, baz: 1 });
});

const relationIndex = createValkeyIndex(
  {
    valkey,
    name: "relation",
    exemplar: 0 as Exemplar<TestObject>,
    relations: ["bar", "baz"],
    get: getHash(),
    set: setHash(),
    update: updateHash(),
  },
  {
    use: async ({ get, update }, pkey: string) => {
      const val = await get!(pkey);
      if (val?.baz === undefined) {
        return;
      }
      const next = val.baz + 1;
      await update!({ pkey, input: { baz: next } });
      return next;
    },
  },
);

test("Relation index", async () => {
  expect(await relationIndex.get("1")).toEqual({});

  await relationIndex.set({
    pkey: "1",
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  expect(await relationIndex.get("1")).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);

  await relationIndex.set({ pkey: "1", input: { foo: "lalala", bar: 0 } });
  expect(await relationIndex.get("1")).toEqual({ foo: "lalala", bar: 0 });
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);

  await relationIndex.set({
    pkey: "1",
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  expect(await relationIndex.get("1")).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);

  await relationIndex.set({
    pkey: "1",
    input: { foo: "lalala", bar: 0, baz: 4 },
  });
  expect(await relationIndex.get("1")).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual([]);

  expect(await relationIndex.f.use("1")).toEqual(5);
  expect(await relationIndex.get("1")).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.pkeysVia("bar", 0)).toEqual(["1"]);
  expect(await relationIndex.pkeysVia("baz", 2)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 4)).toEqual([]);
  expect(await relationIndex.pkeysVia("baz", 5)).toEqual(["1"]);
});

const streamIndex = createValkeyIndex(
  {
    valkey,
    name: "stream",
    exemplar: 0 as Exemplar<TestObject>,
    relations: [],
  },
  {
    append: appendStream(),
    range: rangeStream(),
    read: readStream(),
  },
);

test("Stream index", async () => {
  // expect(await streamIndex.get("1")).toEqual({});
});
