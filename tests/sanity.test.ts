import { createValkeyIndex, type Exemplar } from "../src";
import { useBeforeEach, valkey } from "./index.test";

useBeforeEach();

const bad_names = ["bad:name", "bad@name", "bad-name"] as const;

test("Sanity checks", () => {
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
      name: "good/name",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good/name.property",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good/name",
      valkey,
      exemplar: { foo: 0 },
      relations: ["foo"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good/name",
      valkey,
      exemplar: { foo: 0, bar: 0 },
      relations: ["foo", "bar"],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good/name",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);

  expect(() => {
    createValkeyIndex({
      name: "good/name.property",
      valkey,
      exemplar: undefined,
      relations: [],
    });
  }).not.toThrow(Error);
});
