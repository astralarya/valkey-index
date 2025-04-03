import { ValkeyIndexType, ValkeyListIndex } from "../src";
import { useBeforeEach, valkey, type TestObject } from "./index.test";

useBeforeEach();

const listIndex = ValkeyListIndex({
  valkey,
  name: "list",
  type: ValkeyIndexType<TestObject>(),
});

test("List", async () => {
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(0);

  await listIndex.push({ pkey: 1, input: { foo: "ababa", bar: 0 } });
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(1);

  await listIndex.push({ pkey: 1, input: { foo: "lalala", bar: 1 } });
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual({
    foo: "lalala",
    bar: 1,
  });
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(2);

  await listIndex.rpush({ pkey: 1, input: { foo: "gagaga", bar: 2 } });
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual({
    foo: "lalala",
    bar: 1,
  });
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual({
    foo: "gagaga",
    bar: 2,
  });
  expect(await listIndex.len({ pkey: 1 })).toEqual(3);

  expect(await listIndex.pop({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 1,
  });
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual({
    foo: "gagaga",
    bar: 2,
  });
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(2);

  expect(await listIndex.rpop({ pkey: 1 })).toEqual({
    foo: "gagaga",
    bar: 2,
  });
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(1);
});
