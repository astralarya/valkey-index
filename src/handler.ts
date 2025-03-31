export type ValkeyIndexHandler<Ops, Args extends unknown[], Return> = (
  ops: Ops,
  ...args: Args
) => Return;

export type ValkeyIndexSpec<Ops> = Record<
  string,
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  ValkeyIndexHandler<Ops, any, any>
>;

export type ValkeyIndexFunction<
  Ops,
  Spec extends ValkeyIndexSpec<Ops>,
  Key extends keyof Spec,
> = Spec[Key] extends ValkeyIndexHandler<Ops, infer Args, infer Return>
  ? (...arg: Args) => Return
  : never;

export type ValkeyIndexInterface<Ops, Spec extends ValkeyIndexSpec<Ops>> = {
  [Key in keyof Spec]: ValkeyIndexFunction<Ops, Spec, Key>;
};

export function bindHandlers<Ops, Spec extends ValkeyIndexSpec<Ops>>(
  ops: Ops,
  spec: Spec,
) {
  return Object.entries(spec).reduce((prev, [key, val]) => {
    prev[key as keyof Spec] = ((
      ...args: Parameters<ValkeyIndexFunction<Ops, Spec, typeof key>>
    ) => {
      return val(ops, ...args);
    }) as ValkeyIndexFunction<Ops, Spec, typeof key>;
    return prev;
  }, {} as ValkeyIndexInterface<Ops, Spec>);
}
