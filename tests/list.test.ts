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

  const pipeline3 = ValkeyPipeline(valkey);
  pipeline3.add(
    "push",
    listIndex.pipe.push({ pkey: 1, input: { foo: "lalala", bar: 1 } }),
  );
  pipeline3.add("index", listIndex.pipe.index({ pkey: 1, index: 0 }));
  pipeline3.add("index", listIndex.pipe.index({ pkey: 1, index: 1 }));
  pipeline3.add("index", listIndex.pipe.index({ pkey: 1, index: 2 }));
  pipeline3.add("len", listIndex.pipe.len({ pkey: 1 }));
  expect(await pipeline3.exec()).toEqual({
    push: [2],
    index: [
      {
        foo: "lalala",
        bar: 1,
      },
      {
        foo: "ababa",
        bar: 0,
      },
      null,
    ],
    len: [2],
  });

  const pipeline4 = ValkeyPipeline(valkey);
  pipeline4.add(
    "rpush",
    listIndex.pipe.rpush({ pkey: 1, input: { foo: "gagaga", bar: 2 } }),
  );
  pipeline4.add("index", listIndex.pipe.index({ pkey: 1, index: 0 }));
  pipeline4.add("index", listIndex.pipe.index({ pkey: 1, index: 1 }));
  pipeline4.add("index", listIndex.pipe.index({ pkey: 1, index: 2 }));
  pipeline4.add("len", listIndex.pipe.len({ pkey: 1 }));
  expect(await pipeline4.exec()).toEqual({
    rpush: [3],
    index: [
      {
        foo: "lalala",
        bar: 1,
      },
      {
        foo: "ababa",
        bar: 0,
      },
      {
        foo: "gagaga",
        bar: 2,
      },
    ],
    len: [3],
  });

  const pipeline5 = ValkeyPipeline(valkey);
  pipeline5.add("pop", listIndex.pipe.pop({ pkey: 1 }));
  pipeline5.add("index", listIndex.pipe.index({ pkey: 1, index: 0 }));
  pipeline5.add("index", listIndex.pipe.index({ pkey: 1, index: 1 }));
  pipeline5.add("index", listIndex.pipe.index({ pkey: 1, index: 2 }));
  pipeline5.add("len", listIndex.pipe.len({ pkey: 1 }));
  expect(await pipeline5.exec()).toEqual({
    pop: [
      {
        foo: "lalala",
        bar: 1,
      },
    ],
    index: [
      {
        foo: "ababa",
        bar: 0,
      },
      {
        foo: "gagaga",
        bar: 2,
      },
      null,
    ],
    len: [2],
  });

  const pipeline6 = ValkeyPipeline(valkey);
  pipeline6.add("rpop", listIndex.pipe.rpop({ pkey: 1 }));
  pipeline6.add("index", listIndex.pipe.index({ pkey: 1, index: 0 }));
  pipeline6.add("index", listIndex.pipe.index({ pkey: 1, index: 1 }));
  pipeline6.add("index", listIndex.pipe.index({ pkey: 1, index: 2 }));
  pipeline6.add("len", listIndex.pipe.len({ pkey: 1 }));
  expect(await pipeline6.exec()).toEqual({
    rpop: [
      {
        foo: "gagaga",
        bar: 2,
      },
    ],
    index: [
      {
        foo: "ababa",
        bar: 0,
      },
      null,
      null,
    ],
    len: [1],
  });
});
