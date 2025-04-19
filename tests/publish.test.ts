import { sleep } from "bun";
import { ValkeyHashIndex, ValkeyType } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";
import { ValkeyPipeline } from "../src/pipeline";

useBeforeEach();

const publishIndex = ValkeyHashIndex({
  valkey,
  name: "publish",
  type: ValkeyType<TestObject>(),
  relations: ["bar", "baz"],
  functions: {
    use: async ({ get, update }, pkey: string) => {
      const val = await get!({ pkey });
      if (val?.baz === undefined) {
        return;
      }
      const next = val.baz + 1;
      await update!({ pkey, input: { baz: next } });
      return next;
    },
  },
});

test("Publish", async () => {
  const subscribe1 = publishIndex.subscribe({ pkey: 1 });
  const subscribe2 = publishIndex.subscribe({
    fkey: 2,
    relation: "bar",
  });
  const subscribe3_1 = publishIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });

  const next1_1 = subscribe1.next();
  const next2_1 = subscribe2.next();
  const next3_1 = subscribe3_1.next();

  await publishIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 2 },
  });

  setTimeout(async () => {
    await publishIndex.publish({
      pkey: 1,
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
          index: "publish",
        },
        message: "hello",
      },
    },
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "hello",
      },
    },
    undefined,
  ]);

  const next1_2 = subscribe1.next();
  const next2_2 = subscribe2.next();
  const subscribe3_2 = publishIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });
  const next3_2 = subscribe3_2.next();

  setTimeout(async () => {
    await publishIndex.publish({
      pkey: 1,
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
          index: "publish",
        },
        message: "world",
      },
    },
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "world",
      },
    },
    undefined,
  ]);

  const next1_3 = subscribe1.next();
  const next2_3 = subscribe2.next();
  const subscribe3_3 = publishIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });
  const next3_3 = subscribe3_3.next();

  await publishIndex.set({
    pkey: 1,
    input: { foo: "gagaga", bar: 3 },
  });

  setTimeout(async () => {
    await publishIndex.publish({
      pkey: 1,
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
          index: "publish",
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
          index: "publish",
        },
        message: "good",
      },
    },
  ]);

  const next1_4 = subscribe1.next();
  const subscribe2_2 = publishIndex.subscribe({
    fkey: 2,
    relation: "bar",
  });
  const next2_4 = subscribe2_2.next();
  const next3_4 = subscribe3_3.next();

  setTimeout(async () => {
    await publishIndex.publish({
      pkey: 1,
      message: "morning",
    });
  }, 10);

  expect(
    await Promise.all([
      Promise.race([next1_4, sleep(100)]),
      Promise.race([next2_4, sleep(100)]),
      Promise.race([next3_4, sleep(100)]),
    ]),
  ).toEqual([
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "morning",
      },
    },
    undefined,
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "morning",
      },
    },
  ]);
}, 10000);

test("Publish pipeline", async () => {
  const subscribe1 = publishIndex.subscribe({ pkey: 1 });
  const subscribe2 = publishIndex.subscribe({
    fkey: 2,
    relation: "bar",
  });
  const subscribe3_1 = publishIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });

  const next1_1 = subscribe1.next();
  const next2_1 = subscribe2.next();
  const next3_1 = subscribe3_1.next();

  const input1 = { foo: "ababa", bar: 2 };
  await publishIndex.set({
    pkey: 1,
    input: input1,
  });

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "publish",
      publishIndex.pipe.publish({
        pkey: 1,
        message: "hello",
        related: publishIndex.related(input1),
      }),
    );
    await pipeline.exec();
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
          index: "publish",
        },
        message: "hello",
      },
    },
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "hello",
      },
    },
    undefined,
  ]);

  const next1_2 = subscribe1.next();
  const next2_2 = subscribe2.next();
  const subscribe3_2 = publishIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });
  const next3_2 = subscribe3_2.next();

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "publish",
      publishIndex.pipe.publish({
        pkey: 1,
        message: "world",
        related: publishIndex.related(input1),
      }),
    );
    await pipeline.exec();
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
          index: "publish",
        },
        message: "world",
      },
    },
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "world",
      },
    },
    undefined,
  ]);

  const next1_3 = subscribe1.next();
  const next2_3 = subscribe2.next();
  const subscribe3_3 = publishIndex.subscribe({
    fkey: 3,
    relation: "bar",
  });
  const next3_3 = subscribe3_3.next();

  const input2 = { foo: "gagaga", bar: 3 };
  await publishIndex.set({
    pkey: 1,
    input: input2,
  });

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "publish",
      publishIndex.pipe.publish({
        pkey: 1,
        message: "good",
        related: publishIndex.related(input2),
      }),
    );
    await pipeline.exec();
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
          index: "publish",
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
          index: "publish",
        },
        message: "good",
      },
    },
  ]);

  const next1_4 = subscribe1.next();
  const subscribe2_2 = publishIndex.subscribe({
    fkey: 2,
    relation: "bar",
  });
  const next2_4 = subscribe2_2.next();
  const next3_4 = subscribe3_3.next();

  setTimeout(async () => {
    const pipeline = ValkeyPipeline(valkey);
    pipeline.add(
      "publish",
      publishIndex.pipe.publish({
        pkey: 1,
        message: "morning",
        related: publishIndex.related(input2),
      }),
    );
    await pipeline.exec();
  }, 10);

  expect(
    await Promise.all([
      Promise.race([next1_4, sleep(100)]),
      Promise.race([next2_4, sleep(100)]),
      Promise.race([next3_4, sleep(100)]),
    ]),
  ).toEqual([
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "morning",
      },
    },
    undefined,
    {
      done: false,
      value: {
        source: {
          pkey: "1",
          index: "publish",
        },
        message: "morning",
      },
    },
  ]);
}, 10000);
