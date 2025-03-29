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

  await streamIndex.append({
    pkey: "1",
    input: { foo: "ababa", bar: 0 },
  });

  const x = await streamIndex.range({ pkey: "1" });
  expect(typeof x[0]?.id).toBe("string");
  expect(x).toMatchObject([{ data: { foo: "ababa", bar: 0 } }]);
});
