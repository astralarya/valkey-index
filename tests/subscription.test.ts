import { createValkeyIndex, getHash, setHash, updateHash } from "../src";
import { useBeforeEach, valkey, type TestObject } from "./index.test";

useBeforeEach();

const subscriptionIndex = createValkeyIndex(
  {
    valkey,
    name: "subscription",
    exemplar: 0 as TestObject | 0,
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

test("Subscription", async () => {
  expect(await subscriptionIndex.get("1")).toEqual({});
});
