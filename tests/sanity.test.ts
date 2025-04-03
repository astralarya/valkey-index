import {
  ValkeyHashIndex,
  ValkeyIndexRecordType,
  ValkeyIndexType,
} from "../src";
import { useBeforeEach, valkey } from "./index.test";

useBeforeEach();

const good_names = ["good_name", "good.name", "good/name"] as const;
const bad_names = [
  "bad:name",
  "bad@name",
  "bad-name",
  "bad?name",
  "bad&name",
  "bad=name",
  "bad\\name",
] as const;

test("Sanity", () => {
  bad_names.forEach((bad_name) => {
    expect(() => {
      ValkeyHashIndex({
        valkey,
        name: bad_name,
        type: ValkeyIndexRecordType<any>(),
        relations: [],
      });
    }).toThrow(Error);
  });

  bad_names.forEach((bad_name, idx) => {
    expect(() => {
      ValkeyHashIndex({
        valkey,
        name: "good_name",
        type: ValkeyIndexRecordType<any>(),
        relations: [bad_name],
      });
    }).toThrow(Error);
  });

  good_names.forEach((good_name) => {
    expect(() => {
      ValkeyHashIndex({
        name: good_name,
        valkey,
        type: ValkeyIndexRecordType<any>(),
        relations: [],
      });
    }).not.toThrow(Error);
    good_names.forEach((good_relation) => {
      expect(() => {
        ValkeyHashIndex({
          name: good_name,
          valkey,
          type: ValkeyIndexRecordType<any>(),
          relations: [good_relation],
        });
      }).not.toThrow(Error);
      good_names.forEach((good_relation2) => {
        expect(() => {
          ValkeyHashIndex({
            name: good_name,
            valkey,
            type: ValkeyIndexRecordType<any>(),
            relations: [good_relation, good_relation2],
          });
        }).not.toThrow(Error);
      });
    });
  });
});
