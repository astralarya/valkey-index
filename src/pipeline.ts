import type { ChainableCommander } from "iovalkey";

export type ValkeyPipelineProps = {
  pipeline: ChainableCommander;
};

export type ValkeyPipelineAction<T> = (
  pipeline: ChainableCommander,
) => ValkeyPipelineGetter<T>;

export type ValkeyPipelineGetter<T> = (
  results: [error: Error | null, result: unknown][],
) => T;

type ValkeyPipelineGetters = Record<string, ValkeyPipelineGetter<any>[]>;

export function ValkeyPipeline({ pipeline }: ValkeyPipelineProps) {
  const getters: ValkeyPipelineGetters = {};

  function add<T>(label: string, action: ValkeyPipelineAction<T>) {
    const getter = action(pipeline);
    if (label in getters) {
      getters.label?.push(getter);
    } else {
      getters[label] = [getter];
    }
  }

  async function exec<T extends Record<string, any[]>>() {
    const results = await pipeline.exec();
    if (!results) {
      return null;
    }
    const values: Record<string, any[]> = {};
    Object.entries(getters).forEach(([label, list]) => {
      values[label] = list.map((getter) => {
        getter(results);
      });
    });
    return values as T;
  }

  return {
    add,
    exec,
  };
}
