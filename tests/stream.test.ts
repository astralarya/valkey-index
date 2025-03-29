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
} from "../src";

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
