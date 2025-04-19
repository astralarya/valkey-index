import { ValkeyHashIndex, ValkeyType, type KeyPart } from "../src";
import { useBeforeEach, valkey, type TestObject } from ".";
import { ValkeyPipeline } from "../src/pipeline";
import type { ChainableCommander } from "iovalkey";

useBeforeEach();

const hashIndex = ValkeyHashIndex({
  valkey,
  name: "hash",
  type: ValkeyType<TestObject>(),
  relations: [],
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

test("Hash", async () => {
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});
  expect(await hashIndex.get({ pkey: 2 })).toEqual({});

  await hashIndex.set({ pkey: 1, input: { foo: "ababa", bar: 0, baz: 1 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 1, fields: ["foo", "bar"] })).toEqual({
    foo: "ababa",
    bar: 0,
  });
  expect(await hashIndex.get({ pkey: 1, fields: ["baz", "bar"] })).toEqual({
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({});

  await hashIndex.set({ pkey: 2, input: { foo: "falala", bar: 10, baz: 11 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "ababa",
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.set({ pkey: 1, input: { foo: "lalala", bar: 0 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 2 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 2,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 4 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 4,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: undefined } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  expect(await hashIndex.f.use({ pkey: 1 })).toEqual(undefined);
  expect(await hashIndex.get({ pkey: 1 })).toEqual({ foo: "lalala", bar: 0 });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.update({ pkey: 1, input: { baz: 0 } });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 0,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  expect(await hashIndex.f.use({ pkey: 1 })).toEqual(1);
  expect(await hashIndex.get({ pkey: 1 })).toEqual({
    foo: "lalala",
    bar: 0,
    baz: 1,
  });
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.del({ pkey: 1 });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});
  expect(await hashIndex.get({ pkey: 2 })).toEqual({
    foo: "falala",
    bar: 10,
    baz: 11,
  });

  await hashIndex.del({ pkey: 2 });
  expect(await hashIndex.get({ pkey: 1 })).toEqual({});
  expect(await hashIndex.get({ pkey: 2 })).toEqual({});
});

test("Hash pipeine", async () => {
  const pipeline1 = ValkeyPipeline(valkey);
  pipeline1.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline1.add("get", hashIndex.pipe.get({ pkey: 2 }));
  expect(await pipeline1.exec()).toEqual({
    get: [{}, {}],
  });

  const pipeline2 = ValkeyPipeline(valkey);
  pipeline2.add(
    "set",
    hashIndex.pipe.set({
      pkey: 1,
      input: { foo: "ababa", bar: 0, baz: 1 },
      prev: undefined,
    }),
  );
  pipeline2.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline2.add("get", hashIndex.pipe.get({ pkey: 1, fields: ["foo", "bar"] }));
  pipeline2.add("get", hashIndex.pipe.get({ pkey: 1, fields: ["bar", "baz"] }));
  pipeline2.add("get", hashIndex.pipe.get({ pkey: 2 }));
  expect(await pipeline2.exec()).toEqual({
    set: [undefined],
    get: [
      { foo: "ababa", bar: 0, baz: 1 },
      {
        foo: "ababa",
        bar: 0,
      },
      {
        bar: 0,
        baz: 1,
      },
      {},
    ],
  });

  const pipeline3 = ValkeyPipeline(valkey);
  pipeline3.add(
    "set",
    hashIndex.pipe.set({
      pkey: 2,
      input: { foo: "falala", bar: 10, baz: 11 },
      prev: undefined,
    }),
  );
  pipeline3.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline3.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result3 = await pipeline3.exec();
  expect(result3).toEqual({
    set: [undefined],
    get: [
      {
        foo: "ababa",
        bar: 0,
        baz: 1,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
  });

  const pipeline4 = ValkeyPipeline(valkey);
  pipeline4.add(
    "set",
    hashIndex.pipe.set({
      pkey: 1,
      input: { foo: "lalala", bar: 0 },
      prev: result3?.get?.[0],
    }),
  );
  pipeline4.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline4.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result4 = await pipeline4.exec();
  expect(result4).toEqual({
    set: [undefined],
    get: [
      { foo: "lalala", bar: 0 },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
  });

  const pipeline5 = ValkeyPipeline(valkey);
  pipeline5.add(
    "update",
    hashIndex.pipe.update({
      pkey: 1,
      input: { baz: 2 },
      prev: result4?.get?.[0],
    }),
  );
  pipeline5.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline5.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result5 = await pipeline5.exec();
  expect(result5).toEqual({
    update: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 2,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
  });

  const pipeline6 = ValkeyPipeline(valkey);
  pipeline6.add(
    "update",
    hashIndex.pipe.update({
      pkey: 1,
      input: { baz: 4 },
      prev: result5?.get?.[0],
    }),
  );
  pipeline6.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline6.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result6 = await pipeline6.exec();
  expect(result6).toEqual({
    update: [undefined],
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
  });

  const pipeline7 = ValkeyPipeline(valkey);
  pipeline7.add(
    "update",
    hashIndex.pipe.update({
      pkey: 1,
      input: { baz: undefined },
      prev: result6?.get?.[0],
    }),
  );
  pipeline7.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline7.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result7 = await pipeline7.exec();
  expect(result7).toEqual({
    update: [undefined],
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
  });

  const pipeline8 = ValkeyPipeline(valkey);
  pipeline8.add(
    "use",
    hashIndex.f.use_pipe({
      pkey: 1,
      prev: result7?.get?.[0],
    }),
  );
  pipeline8.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline8.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result8 = await pipeline8.exec();
  expect(result8).toEqual({
    use: [undefined],
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
  });

  const pipeline9 = ValkeyPipeline(valkey);
  pipeline9.add(
    "update",
    hashIndex.pipe.update({
      pkey: 1,
      input: { baz: 0 },
      prev: result8?.get?.[0],
    }),
  );
  pipeline9.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline9.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result9 = await pipeline9.exec();
  expect(result9).toEqual({
    update: [undefined],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 0,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
  });

  const pipeline10 = ValkeyPipeline(valkey);
  pipeline10.add(
    "use",
    hashIndex.f.use_pipe({
      pkey: 1,
      prev: result9?.get?.[0],
    }),
  );
  pipeline10.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline10.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result10 = await pipeline10.exec();
  expect(result10).toEqual({
    use: [1],
    get: [
      {
        foo: "lalala",
        bar: 0,
        baz: 1,
      },
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
  });

  const pipeline11 = ValkeyPipeline(valkey);
  pipeline11.add(
    "del",
    hashIndex.pipe.del({
      pkey: 1,
      related: hashIndex.related(result10?.get?.[0]),
    }),
  );
  pipeline11.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline11.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result11 = await pipeline11.exec();
  expect(result11).toEqual({
    del: [undefined],
    get: [
      {},
      {
        foo: "falala",
        bar: 10,
        baz: 11,
      },
    ],
  });

  const pipeline12 = ValkeyPipeline(valkey);
  pipeline12.add(
    "del",
    hashIndex.pipe.del({
      pkey: 2,
      related: hashIndex.related(result11?.get?.[1]),
    }),
  );
  pipeline12.add("get", hashIndex.pipe.get({ pkey: 1 }));
  pipeline12.add("get", hashIndex.pipe.get({ pkey: 2 }));
  const result12 = await pipeline12.exec();
  expect(result12).toEqual({
    del: [undefined],
    get: [{}, {}],
  });
});
