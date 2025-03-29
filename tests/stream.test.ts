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
  // expect(await streamIndex.get("1")).toEqual({});
});
