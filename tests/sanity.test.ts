import { createValkeyIndex, type Exemplar } from "../src";
import { useBeforeEach, valkey } from "./index.test";

useBeforeEach();

test("Sanity checks", () => {
  const bad_parent_names = ["bad:name", "bad@name", "bad-name"] as const;
  const bad_names = [...bad_parent_names, "bad/name"] as const;

  bad_names.forEach((bad_name) => {
    expect(() => {
      createValkeyIndex({
        valkey,
        name: bad_name,
        exemplar: undefined,
        relations: [],
      });
    }).toThrow(Error);
  });

  bad_names.forEach((bad_name, idx) => {
    expect(() => {
      createValkeyIndex({
        valkey,
        name: "good_name",
        exemplar: 0 as Exemplar<Record<(typeof bad_names)[typeof idx], 0>>,
        relations: [bad_name],
      });
    }).toThrow(Error);
  });

  bad_parent_names.forEach((bad_name) => {
    expect(() => {
      createValkeyIndex({
        valkey,
        parent: bad_name,
        name: "good_name",
        exemplar: undefined,
        relations: [],
      });
    }).toThrow(Error);
  });

  expect(() => {
    createValkeyIndex({
      name: "good_name",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name.property",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name.property",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good_name.property",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good_parent",
      name: "good_name",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good_parent",
      name: "good_name.property",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good_parent",
      name: "good_name",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good_parent",
      name: "good_name",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good/parent",
      name: "good_name",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good/parent",
      name: "good_name.property",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good/parent",
      name: "good_name",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      parent: "good/parent",
      name: "good_name",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);
});
