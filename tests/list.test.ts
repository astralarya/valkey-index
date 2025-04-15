import { ValkeyType, ValkeyListIndex } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";
import { ValkeyPipeline } from "../src/pipeline";

useBeforeEach();

const listIndex = ValkeyListIndex({
  valkey,
  name: "list",
  type: ValkeyType<TestObject>(),
});

test("List", async () => {
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(0);

  expect(
    await listIndex.push({ pkey: 1, input: { foo: "ababa", bar: 0 } }),
  ).toEqual(1);
  expect(await listIndex.index({ pkey: 1, index: 0 })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await listIndex.index({ pkey: 1, index: 1 })).toEqual(null);
  expect(await listIndex.index({ pkey: 1, index: 2 })).toEqual(null);
  expect(await listIndex.len({ pkey: 1 })).toEqual(1);

  expect(
    await listIndex.push({ pkey: 1, input: { foo: "lalala", bar: 1 } }),
  ).toEqual(2);
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

  expect(
    await listIndex.rpush({ pkey: 1, input: { foo: "gagaga", bar: 2 } }),
  ).toEqual(3);
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

test("List pipeline", async () => {
  const pipeline1 = ValkeyPipeline(valkey);
  pipeline1.add("index", listIndex.pipe.index({ pkey: 1, index: 0 }));
  pipeline1.add("index", listIndex.pipe.index({ pkey: 1, index: 1 }));
  pipeline1.add("index", listIndex.pipe.index({ pkey: 1, index: 2 }));
  pipeline1.add("len", listIndex.pipe.len({ pkey: 1 }));
  expect(await pipeline1.exec()).toEqual({
    index: [null, null, null],
    len: [0],
  });

  const pipeline2 = ValkeyPipeline(valkey);
  pipeline2.add(
    "push",
    listIndex.pipe.push({ pkey: 1, input: { foo: "ababa", bar: 0 } }),
  );
  pipeline2.add("index", listIndex.pipe.index({ pkey: 1, index: 0 }));
  pipeline2.add("index", listIndex.pipe.index({ pkey: 1, index: 1 }));
  pipeline2.add("index", listIndex.pipe.index({ pkey: 1, index: 2 }));
  pipeline2.add("len", listIndex.pipe.len({ pkey: 1 }));
  expect(await pipeline2.exec()).toEqual({
    push: [1],
    index: [{ foo: "ababa", bar: 0 }, null, null],
    len: [1],
  });
});
