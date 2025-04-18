import { ValkeyType, ValkeyStreamIndex } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";
import { ValkeyPipeline } from "../src/pipeline";

useBeforeEach();

const streamIndex = ValkeyStreamIndex({
  valkey,
  name: "stream",
  type: ValkeyType<TestObject>(),
});

test("Stream", async () => {
  expect(await streamIndex.range({ pkey: 1 })).toEqual([]);

  setTimeout(async () => {
    await streamIndex.append({
      pkey: 1,
      input: { foo: "ababa", bar: 0 },
    });
  }, 10);

  const read1 = await streamIndex.read({ pkey: 1 });

  const next1_1 = (await read1.next()).value;
  expect(typeof next1_1.id).toBe("string");
  expect(next1_1).toMatchObject({
    data: { foo: "ababa", bar: 0 },
  });

  const range1 = await streamIndex.range({ pkey: 1 });
  expect(typeof range1[0]?.id).toBe("string");
  expect(range1).toMatchObject([{ data: { foo: "ababa", bar: 0 } }]);

  setTimeout(async () => {
    await streamIndex.append({
      pkey: 1,
      input: { foo: "lalala", bar: 1 },
    });
  }, 10);

  const next1_2 = (await read1.next()).value;
  expect(typeof next1_2.id).toBe("string");
  expect(next1_2).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });

  const range2 = await streamIndex.range({ pkey: 1 });
  expect(typeof range2[0]?.id).toBe("string");
  expect(range2).toMatchObject([
    { data: { foo: "ababa", bar: 0 } },
    { data: { foo: "lalala", bar: 1 } },
  ]);

  const continue1 = await streamIndex.range({
    pkey: 1,
    start: range2[range2.length - 1]?.id,
  });
  expect(typeof continue1[0]?.id).toBe("string");
  expect(continue1).toMatchObject([{ data: { foo: "lalala", bar: 1 } }]);

  const read2 = await streamIndex.read({ pkey: 1, lastId: next1_1.id });
  const next2_1 = (await read2.next()).value;
  expect(typeof next2_1.id).toBe("string");
  expect(next2_1).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });

  setTimeout(async () => {
    await streamIndex.append({
      pkey: 1,
      input: { foo: "gagaga", bar: 2 },
    });
  }, 10);

  const next1_3 = (await read1.next()).value;
  expect(typeof next1_3.id).toBe("string");
  expect(next1_3).toMatchObject({
    data: { foo: "gagaga", bar: 2 },
  });

  const next2_2 = (await read2.next()).value;
  expect(typeof next2_2.id).toBe("string");
  expect(next2_2).toMatchObject({
    data: { foo: "gagaga", bar: 2 },
  });

  const readbegin = await streamIndex.read({
    pkey: 1,
    count: 10,
    lastId: "0",
  });

  const nextbegin_1 = (await readbegin.next()).value;
  expect(typeof nextbegin_1.id).toBe("string");
  expect(nextbegin_1).toMatchObject({
    data: { foo: "ababa", bar: 0 },
  });

  const nextbegin_2 = (await readbegin.next()).value;
  expect(typeof nextbegin_2.id).toBe("string");
  expect(nextbegin_2).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });
});

test("Stream abort", async () => {
  expect(await streamIndex.range({ pkey: 1 })).toEqual([]);

  const controller = new AbortController();

  const read1 = await streamIndex.read({
    pkey: 1,
    signal: controller.signal,
  });

  setTimeout(async () => {
    controller.abort();
  }, 10);

  const mock = jest.spyOn(console, "error").mockImplementation(() => {});
  expect(await read1.next()).toMatchObject({ done: true });
  mock.mockRestore();
});

test("Stream pipeline", async () => {
  const pipeline1 = ValkeyPipeline(valkey);
  pipeline1.add("range", streamIndex.pipe.range({ pkey: 1 }));
  expect(await pipeline1.exec()).toEqual({ range: [[]] });

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "append",
      streamIndex.pipe.append({
        pkey: 1,
        input: { foo: "ababa", bar: 0 },
      }),
    );
    await pipeline.exec();
  }, 10);

  const read1 = await streamIndex.read({ pkey: 1 });

  const next1_1 = (await read1.next()).value;
  expect(typeof next1_1.id).toBe("string");
  expect(next1_1).toMatchObject({
    data: { foo: "ababa", bar: 0 },
  });

  const pipeline2 = ValkeyPipeline(valkey);
  pipeline2.add("range", streamIndex.pipe.range({ pkey: 1 }));
  const {
    range: [range1],
  } = await pipeline2.exec<any>();
  expect(typeof range1[0]?.id).toBe("string");
  expect(range1).toMatchObject([{ data: { foo: "ababa", bar: 0 } }]);

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "append",
      streamIndex.pipe.append({
        pkey: 1,
        input: { foo: "lalala", bar: 1 },
      }),
    );
    await pipeline.exec();
  }, 10);

  const next1_2 = (await read1.next()).value;
  expect(typeof next1_2.id).toBe("string");
  expect(next1_2).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });

  const pipeline3 = ValkeyPipeline(valkey);
  pipeline3.add("range", streamIndex.pipe.range({ pkey: 1 }));
  const {
    range: [range2],
  } = await pipeline3.exec<any>();
  expect(typeof range2[0]?.id).toBe("string");
  expect(range2).toMatchObject([
    { data: { foo: "ababa", bar: 0 } },
    { data: { foo: "lalala", bar: 1 } },
  ]);

  const pipeline4 = ValkeyPipeline(valkey);
  pipeline4.add(
    "range",
    streamIndex.pipe.range({ pkey: 1, start: range2[range2.length - 1]?.id }),
  );
  const {
    range: [continue1],
  } = await pipeline4.exec<any>();
  expect(typeof continue1[0]?.id).toBe("string");
  expect(continue1).toMatchObject([{ data: { foo: "lalala", bar: 1 } }]);

  const read2 = await streamIndex.read({ pkey: 1, lastId: next1_1.id });
  const next2_1 = (await read2.next()).value;
  expect(typeof next2_1.id).toBe("string");
  expect(next2_1).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "append",
      streamIndex.pipe.append({
        pkey: 1,
        input: { foo: "gagaga", bar: 2 },
      }),
    );
    await pipeline.exec();
  }, 10);

  const next1_3 = (await read1.next()).value;
  expect(typeof next1_3.id).toBe("string");
  expect(next1_3).toMatchObject({
    data: { foo: "gagaga", bar: 2 },
  });

  const next2_2 = (await read2.next()).value;
  expect(typeof next2_2.id).toBe("string");
  expect(next2_2).toMatchObject({
    data: { foo: "gagaga", bar: 2 },
  });

  const readbegin = await streamIndex.read({
    pkey: 1,
    count: 10,
    lastId: "0",
  });

  const nextbegin_1 = (await readbegin.next()).value;
  expect(typeof nextbegin_1.id).toBe("string");
  expect(nextbegin_1).toMatchObject({
    data: { foo: "ababa", bar: 0 },
  });

  const nextbegin_2 = (await readbegin.next()).value;
  expect(typeof nextbegin_2.id).toBe("string");
  expect(nextbegin_2).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });
});
