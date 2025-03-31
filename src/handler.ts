export type ValkeyIndexHandler<O, A extends [], V> = (ops: O, ...args: A) => V;

export type ValkeyIndexSpec = Record<
  string,
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  ValkeyIndexHandler<any, any, any>
>;

export type ValkeyIndexFunction<
  M extends ValkeyIndexSpec,
  K extends keyof M,
> = M[K] extends ValkeyIndexHandler<infer O, infer A, infer V>
  ? (ops: O, ...arg: A) => V
  : never;

export type ValkeyIndexInterface<M extends ValkeyIndexSpec> = {
  [K in keyof M]: ValkeyIndexFunction<M, K>;
};

// const func = Object.entries(functions).reduce((prev, [key, val]) => {
//   prev[key as keyof M] = ((
//     arg: Parameters<ValkeyIndexFunction<T, R, M, typeof key>>[1],
//   ) => {
//     return val(ops, arg);
//   }) as ValkeyIndexFunction<T, R, M, typeof key>;
//   return prev;
// }, {} as ValkeyIndexInterface<T, R, M>);
