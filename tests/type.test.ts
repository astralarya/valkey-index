import { ValkeyType, ValkeyListIndex } from "../src";
import { useBeforeEach, valkey } from ".";

useBeforeEach();

const stringListIndex = ValkeyListIndex({
  valkey,
  name: "string",
  type: ValkeyType(String),
});

const numberListIndex = ValkeyListIndex({
  valkey,
  name: "number",
  type: ValkeyType(Number),
});

test("String", async () => {
  expect(await stringListIndex.index({ pkey: 1, index: 0 })).toEqual(null);
  expect(await stringListIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await stringListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await stringListIndex.len({ pkey: 1 })).toEqual(0);

  await stringListIndex.push({ pkey: 1, input: "foo" });
  expect(await stringListIndex.index({ pkey: 1, index: 0 })).toEqual("foo");
  expect(await stringListIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await stringListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await stringListIndex.len({ pkey: 1 })).toEqual(1);

  await stringListIndex.push({ pkey: 1, input: "bar" });
  expect(await stringListIndex.index({ pkey: 1, index: 0 })).toEqual("bar");
  expect(await stringListIndex.index({ pkey: 1, index: 1 })).toEqual("foo");
  expect(await stringListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await stringListIndex.len({ pkey: 1 })).toEqual(2);

  await stringListIndex.rpush({ pkey: 1, input: "baz" });
  expect(await stringListIndex.index({ pkey: 1, index: 0 })).toEqual("bar");
  expect(await stringListIndex.index({ pkey: 1, index: 1 })).toEqual("foo");
  expect(await stringListIndex.index({ pkey: 1, index: 2 })).toEqual("baz");
  expect(await stringListIndex.len({ pkey: 1 })).toEqual(3);

  expect(await stringListIndex.pop({ pkey: 1 })).toEqual("bar");
  expect(await stringListIndex.index({ pkey: 1, index: 0 })).toEqual("foo");
  expect(await stringListIndex.index({ pkey: 1, index: 1 })).toEqual("baz");
  expect(await stringListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await stringListIndex.len({ pkey: 1 })).toEqual(2);

  expect(await stringListIndex.rpop({ pkey: 1 })).toEqual("baz");
  expect(await stringListIndex.index({ pkey: 1, index: 0 })).toEqual("foo");
  expect(await stringListIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await stringListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await stringListIndex.len({ pkey: 1 })).toEqual(1);
});

test("Number", async () => {
  expect(await numberListIndex.index({ pkey: 1, index: 0 })).toEqual(null);
  expect(await numberListIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await numberListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await numberListIndex.len({ pkey: 1 })).toEqual(0);

  await numberListIndex.push({ pkey: 1, input: 1 });
  expect(await numberListIndex.index({ pkey: 1, index: 0 })).toEqual(1);
  expect(await numberListIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await numberListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await numberListIndex.len({ pkey: 1 })).toEqual(1);

  await numberListIndex.push({ pkey: 1, input: 2 });
  expect(await numberListIndex.index({ pkey: 1, index: 0 })).toEqual(2);
  expect(await numberListIndex.index({ pkey: 1, index: 1 })).toEqual(1);
  expect(await numberListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await numberListIndex.len({ pkey: 1 })).toEqual(2);

  await numberListIndex.rpush({ pkey: 1, input: 3 });
  expect(await numberListIndex.index({ pkey: 1, index: 0 })).toEqual(2);
  expect(await numberListIndex.index({ pkey: 1, index: 1 })).toEqual(1);
  expect(await numberListIndex.index({ pkey: 1, index: 2 })).toEqual(3);
  expect(await numberListIndex.len({ pkey: 1 })).toEqual(3);

  expect(await numberListIndex.pop({ pkey: 1 })).toEqual(2);
  expect(await numberListIndex.index({ pkey: 1, index: 0 })).toEqual(1);
  expect(await numberListIndex.index({ pkey: 1, index: 1 })).toEqual(3);
  expect(await numberListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await numberListIndex.len({ pkey: 1 })).toEqual(2);

  expect(await numberListIndex.rpop({ pkey: 1 })).toEqual(3);
  expect(await numberListIndex.index({ pkey: 1, index: 0 })).toEqual(1);
  expect(await numberListIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await numberListIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await numberListIndex.len({ pkey: 1 })).toEqual(1);
});
