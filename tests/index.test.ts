import Valkey from "iovalkey";

export const valkey = new Valkey({
  username: process.env.VKUSERNAME,
  password: process.env.VKPASSWORD,
  host: process.env.VKHOST,
  port: process.env.VKPORT ? parseInt(process.env.VKPORT) : undefined,
});

export type TestObject = {
  foo: string;
  bar: number;
  baz?: number;
};

export function useBeforeEach() {
  beforeEach(async () => {
    await valkey.flushall();
  });
}
