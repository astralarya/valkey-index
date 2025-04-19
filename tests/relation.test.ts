import type { ChainableCommander } from "iovalkey";

import { ValkeyHashIndex, ValkeyType, type KeyPart } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";
import { ValkeyPipeline } from "../src/pipeline";

useBeforeEach();

const relationIndex = ValkeyHashIndex({
  valkey,
  name: "relation",
  type: ValkeyType<TestObject>(),
  relations: ["bar", "baz"],
  functions: {
    use: async ({ reduce }, { pkey }: { pkey: KeyPart }) => {
      return (
        await reduce({
          pkey,
          reducer: (prev) =>
            prev
              ? {
                  ...prev,
                  baz: prev.baz !== undefined ? prev.baz + 1 : undefined,
                }
              : undefined,
        })
      )?.baz;
    },
    use_pipe: (
      { pipe: { update } },
      { pkey, prev }: { pkey: KeyPart; prev: TestObject },
    ) => {
      return function pipe(pipeline: ChainableCommander) {
        const next = prev.baz !== undefined ? prev.baz + 1 : undefined;
        update({
          pkey,
          input: {
            baz: next,
          },
          prev,
        })(pipeline);
        return function () {
          return next;
        };
      };
    },
  },
});

test("Relations", async () => {
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});

  await relationIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);

  await relationIndex.set({
    pkey: 2,
    input: { foo: "falala", bar: 10, baz: 11 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.set({ pkey: 1, input: { foo: "lalala", bar: 0 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.set({
    pkey: 1,
    input: { foo: "lalala", bar: 0, baz: 4 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  expect(await relationIndex.f.use({ pkey: 1 })).toEqual(5);
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 2, input: { bar: 0 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 0,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 2, input: { bar: 1 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 1, input: { bar: 1 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 1,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "1",
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.update({ pkey: 1, input: { bar: 0 } });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 5,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.del({ pkey: 1 });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 1,
    baz: 11,
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([
    "2",
  ]);

  await relationIndex.del({ fkey: 1, relation: "bar" });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);

  await relationIndex.set({
    pkey: 1,
    input: { foo: "ababa", bar: 0, baz: 2 },
  });
  await relationIndex.set({
    pkey: 2,
    input: { foo: "lalala", bar: 0, baz: 4 },
  });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 2,
  });
  expect(await relationIndex.get({ pkey: 2 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await relationIndex.get({ fkey: 0, relation: "bar" })).toEqual({
    1: {
      foo: "ababa",
      bar: 0,
      baz: 2,
    },
    2: {
      foo: "lalala",
      bar: 0,
      baz: 4,
    },
  });
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([
    "1",
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([
    "1",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([
    "2",
  ]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);

  await relationIndex.del({ fkey: 0, relation: "bar" });
  expect(await relationIndex.get({ pkey: 1 })).toEqual({});
  expect(await relationIndex.get({ pkey: 2 })).toEqual({});
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 0 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 1 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "bar", fkey: 10 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 2 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 4 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 5 })).toEqual([]);
  expect(await relationIndex.pkeys({ relation: "baz", fkey: 11 })).toEqual([]);
});

test("Relations pipeline", async () => {
  const pipeline1 = ValkeyPipeline(valkey);
  pipeline1.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline1.add("get", relationIndex.pipe.get({ pkey: 2 }));
  expect(await pipeline1.exec()).toEqual({
    get: [{}, {}],
  });

  const pipeline2 = ValkeyPipeline(valkey);
  pipeline2.add(
    "set",
    relationIndex.pipe.set({
      pkey: 1,
      input: { foo: "ababa", bar: 0, baz: 2 },
      prev: undefined,
    }),
  );
  pipeline2.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline2.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline2.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  expect(await pipeline2.exec()).toEqual({
    set: [undefined],
    get: [
      {
        foo: "ababa",
        bar: 0,
        baz: 2,
      },
      {},
    ],
    pkeys: [["1"], [], [], ["1"], [], [], []],
  });

  const pipeline3 = ValkeyPipeline(valkey);
  pipeline3.add(
    "set",
    relationIndex.pipe.set({
      pkey: 2,
      input: { foo: "falala", bar: 10, baz: 11 },
      prev: undefined,
    }),
  );
  pipeline3.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline3.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline3.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result3 = await pipeline3.exec();
  expect(result3).toEqual({
    set: [undefined],
    get: [
      {
        foo: "ababa",
        bar: 0,
        baz: 2,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
    pkeys: [["1"], [], ["2"], ["1"], [], [], ["2"]],
  });

  const pipeline4 = ValkeyPipeline(valkey);
  pipeline4.add(
    "set",
    relationIndex.pipe.set({
      pkey: 1,
      input: { foo: "lalala", bar: 0 },
      prev: result3?.get?.[0],
    }),
  );
  pipeline4.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline4.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline4.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result4 = await pipeline4.exec();
  expect(result4).toEqual({
    set: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
    pkeys: [["1"], [], ["2"], [], [], [], ["2"]],
  });

  const pipeline5 = ValkeyPipeline(valkey);
  pipeline5.add(
    "set",
    relationIndex.pipe.set({
      pkey: 1,
      input: { foo: "ababa", bar: 0, baz: 2 },
      prev: result4?.get?.[0],
    }),
  );
  pipeline5.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline5.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline5.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result5 = await pipeline5.exec();
  expect(result5).toEqual({
    set: [undefined],
    get: [
      {
        foo: "ababa",
        bar: 0,
        baz: 2,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
    pkeys: [["1"], [], ["2"], ["1"], [], [], ["2"]],
  });

  const pipeline6 = ValkeyPipeline(valkey);
  pipeline6.add(
    "set",
    relationIndex.pipe.set({
      pkey: 1,
      input: { foo: "lalala", bar: 0, baz: 4 },
      prev: result5?.get?.[0],
    }),
  );
  pipeline6.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline6.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline6.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result6 = await pipeline6.exec();
  expect(result6).toEqual({
    set: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 4,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
    pkeys: [["1"], [], ["2"], [], ["1"], [], ["2"]],
  });

  const pipeline7 = ValkeyPipeline(valkey);
  pipeline7.add(
    "use",
    relationIndex.f.use_pipe({
      pkey: 1,
      prev: result6?.get?.[0],
    }),
  );
  pipeline7.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline7.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline7.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result7 = await pipeline7.exec();
  expect(result7).toEqual({
    use: [5],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 5,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
    pkeys: [["1"], [], ["2"], [], [], ["1"], ["2"]],
  });

  const pipeline8 = ValkeyPipeline(valkey);
  pipeline8.add(
    "update",
    relationIndex.pipe.update({
      pkey: 2,
      input: { bar: 0 },
      prev: result7?.get?.[1],
    }),
  );
  pipeline8.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline8.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline8.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result8 = await pipeline8.exec();
  expect(result8).toEqual({
    update: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 5,
      },
      {
        foo: "falala",
        bar: 0,
        baz: 11,
      },
    ],
    pkeys: [["1", "2"], [], [], [], [], ["1"], ["2"]],
  });

  const pipeline9 = ValkeyPipeline(valkey);
  pipeline9.add(
    "update",
    relationIndex.pipe.update({
      pkey: 2,
      input: { bar: 1 },
      prev: result8?.get?.[1],
    }),
  );
  pipeline9.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline9.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline9.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result9 = await pipeline9.exec();
  expect(result9).toEqual({
    update: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 5,
      },
      {
        foo: "falala",
        bar: 1,
        baz: 11,
      },
    ],
    pkeys: [["1"], ["2"], [], [], [], ["1"], ["2"]],
  });

  const pipeline10 = ValkeyPipeline(valkey);
  pipeline10.add(
    "update",
    relationIndex.pipe.update({
      pkey: 1,
      input: { bar: 1 },
      prev: result9?.get?.[0],
    }),
  );
  pipeline10.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline10.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline10.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result10 = await pipeline10.exec();
  expect(result10).toEqual({
    update: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 1,
        baz: 5,
      },
      {
        foo: "falala",
        bar: 1,
        baz: 11,
      },
    ],
    pkeys: [[], ["1", "2"], [], [], [], ["1"], ["2"]],
  });

  const pipeline11 = ValkeyPipeline(valkey);
  pipeline11.add(
    "update",
    relationIndex.pipe.update({
      pkey: 1,
      input: { bar: 0 },
      prev: result10?.get?.[0],
    }),
  );
  pipeline11.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline11.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline11.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result11 = await pipeline11.exec();
  expect(result11).toEqual({
    update: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 5,
      },
      {
        foo: "falala",
        bar: 1,
        baz: 11,
      },
    ],
    pkeys: [["1"], ["2"], [], [], [], ["1"], ["2"]],
  });

  const pipeline12 = ValkeyPipeline(valkey);
  pipeline12.add(
    "del",
    relationIndex.pipe.del({
      pkey: 1,
      related: relationIndex.related(result11?.get?.[0]),
    }),
  );
  pipeline12.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline12.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline12.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result12 = await pipeline12.exec();
  expect(result12).toEqual({
    del: [undefined],
    get: [
      {},
      {
        foo: "falala",
        bar: 1,
        baz: 11,
      },
    ],
    pkeys: [[], ["2"], [], [], [], [], ["2"]],
  });

  const pipeline13 = ValkeyPipeline(valkey);
  pipeline13.add(
    "del",
    relationIndex.pipe.del({
      pkey: 2,
      related: relationIndex.related(result12?.get?.[1]),
    }),
  );
  pipeline13.add("get", relationIndex.pipe.get({ pkey: 1 }));
  pipeline13.add("get", relationIndex.pipe.get({ pkey: 2 }));
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 0 }),
  );
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 1 }),
  );
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "bar", fkey: 10 }),
  );
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 2 }),
  );
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 4 }),
  );
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 5 }),
  );
  pipeline13.add(
    "pkeys",
    relationIndex.pipe.pkeys({ relation: "baz", fkey: 11 }),
  );
  const result13 = await pipeline13.exec();
  expect(result13).toEqual({
    del: [undefined],
    get: [{}, {}],
    pkeys: [[], [], [], [], [], [], []],
  });
});
