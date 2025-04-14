import type { ChainableCommander } from "iovalkey";

export type ValkeyPipelineProps = {
  pipeline: ChainableCommander;
};

export type ValkeyPipelineAction<T> = (
  pipeline: ChainableCommander,
) => (results: [error: Error | null, result: unknown][]) => T;

export function ValkeyPipeline({ pipeline }: ValkeyPipelineProps) {
  const getters = {};

  function add<T>(label: string, action: ValkeyPipelineAction<T>) {
    const getter = action(pipeline);
  }

  async function exec() {
    const results = await pipeline.exec();
  }

  return {
    add,
    exec,
  };
}
