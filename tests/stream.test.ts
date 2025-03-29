import {
  appendStream,
  createValkeyIndex,
  rangeStream,
  readStream,
  type Exemplar,
} from "../src";
import { useBeforeEach, valkey, type TestObject } from "./index.test";

useBeforeEach();

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
  expect(await streamIndex.range({ pkey: "1" })).toEqual([]);

  setTimeout(async () => {
    await streamIndex.append({
      pkey: "1",
      input: { foo: "ababa", bar: 0 },
    });
  }, 10);

  const read1 = await streamIndex.read({ pkey: "1" });

  const next1_1 = (await read1.next()).value;
  expect(typeof next1_1.id).toBe("string");
  expect(next1_1).toMatchObject({
    data: { foo: "ababa", bar: 0 },
  });

  const range1 = await streamIndex.range({ pkey: "1" });
  expect(typeof range1[0]?.id).toBe("string");
  expect(range1).toMatchObject([{ data: { foo: "ababa", bar: 0 } }]);

  setTimeout(async () => {
    await streamIndex.append({
      pkey: "1",
      input: { foo: "lalala", bar: 1 },
    });
  }, 10);

  const next1_2 = (await read1.next()).value;
  expect(typeof next1_2.id).toBe("string");
  expect(next1_2).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });

  const range2 = await streamIndex.range({ pkey: "1" });
  expect(typeof range2[0]?.id).toBe("string");
  expect(range2).toMatchObject([
    { data: { foo: "ababa", bar: 0 } },
    { data: { foo: "lalala", bar: 1 } },
  ]);

  const continue1 = await streamIndex.range({
    pkey: "1",
    start: range2[range2.length - 1]?.id,
  });
  expect(typeof continue1[0]?.id).toBe("string");
  expect(continue1).toMatchObject([{ data: { foo: "lalala", bar: 1 } }]);

  const read2 = await streamIndex.read({ pkey: "1", lastId: next1_1.id });
  const next2_1 = (await read2.next()).value;
  expect(typeof next2_1.id).toBe("string");
  expect(next2_1).toMatchObject({
    data: { foo: "lalala", bar: 1 },
  });

  setTimeout(async () => {
    await streamIndex.append({
      pkey: "1",
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
    pkey: "1",
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

test("Stream index abort", async () => {
  expect(await streamIndex.range({ pkey: "1" })).toEqual([]);

  const controller = new AbortController();

  const read1 = await streamIndex.read({
    pkey: "1",
    signal: controller.signal,
  });

  setTimeout(async () => {
    controller.abort();
  }, 10);

  const error = console.error;
  jest.spyOn(console, "error").mockImplementation(() => {});
  expect(await read1.next()).toMatchObject({ done: true });
  jest.spyOn(console, "error").mockImplementation(error);
});
