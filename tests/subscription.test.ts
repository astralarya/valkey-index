import { sleep } from "bun";
import { ValkeyHashIndex, ValkeyType } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";

useBeforeEach();

const subscriptionIndex = ValkeyHashIndex({
  valkey,
  name: "subscription",
  type: ValkeyType<TestObject>(),
  relations: ["bar", "baz"],
  functions: {
    use: async ({ get, update }, pkey: string) => {
      const val = await get({ pkey });
      if (val?.baz === undefined) {
        return;
      }
      const next = val.baz + 1;
      await update({ pkey, input: { baz: next } });
      return next;
    },
  },
});

test("Subscription", async () => {
  expect(await subscriptionIndex.get({ pkey: 1 })).toEqual({});

  const subscribe1 = subscriptionIndex.subscribe({ pkey: 1 });
  const subscribe2 = subscriptionIndex.subscribe({
    fkey: 2,
    relation: "bar",
  });
  const subscribe3_1 = subscriptionIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });

  const next1_1 = subscribe1.next();
  const next2_1 = subscribe2.next();
  const next3_1 = subscribe3_1.next();

  setTimeout(async () => {
    await subscriptionIndex.set({
      pkey: 1,
      input: { foo: "ababa", bar: 2 },
      message: "hello",
    });
  }, 10);

  expect(
    await Promise.all([
      Promise.race([next1_1, sleep(100)]),
      Promise.race([next2_1, sleep(100)]),
      Promise.race([next3_1, sleep(100)]),
    ]),
  ).toEqual([
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "subscription",
        },
        message: "hello",
      },
    },
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "subscription",
        },
        message: "hello",
      },
    },
    undefined,
  ]);

  const next1_2 = subscribe1.next();
  const next2_2 = subscribe2.next();
  const subscribe3_2 = subscriptionIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });
  const next3_2 = subscribe3_2.next();

  setTimeout(async () => {
    await subscriptionIndex.set({
      pkey: 1,
      input: { foo: "gagaga", bar: 2 },
      message: "world",
    });
  }, 10);

  expect(
    await Promise.all([
      Promise.race([next1_2, sleep(100)]),
      Promise.race([next2_2, sleep(100)]),
      Promise.race([next3_2, sleep(100)]),
    ]),
  ).toEqual([
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "subscription",
        },
        message: "world",
      },
    },
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "subscription",
        },
        message: "world",
      },
    },
    undefined,
  ]);

  const next1_3 = subscribe1.next();
  const next2_3 = subscribe2.next();
  const subscribe3_3 = subscriptionIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });
  const next3_3 = subscribe3_3.next();

  setTimeout(async () => {
    await subscriptionIndex.update({
      pkey: 1,
      input: { bar: 3 },
      message: "good",
    });
  }, 10);

  expect(
    await Promise.all([
      Promise.race([next1_3, sleep(100)]),
      Promise.race([next2_3, sleep(100)]),
      Promise.race([next3_3, sleep(100)]),
    ]),
  ).toEqual([
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "subscription",
        },
        message: "good",
      },
    },
    undefined,
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "subscription",
        },
        message: "good",
      },
    },
  ]);
}, 10000);
