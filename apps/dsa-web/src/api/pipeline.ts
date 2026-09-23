import apiClient from './index';
import { toCamelCase } from './utils';

export type PipelineRunResponse = { runId: string };
export type PipelineTaskResponse = {
  runId: string;
  status: 'running' | 'completed' | 'failed';
  log: string;
  exitCode: number | null;
};

export const pipelineApi = {
  async run(): Promise<PipelineRunResponse> {
    const r = await apiClient.post<Record<string, unknown>>('/api/v1/pipeline/run');
    return toCamelCase<PipelineRunResponse>(r.data);
  },
  async getTask(runId: string): Promise<PipelineTaskResponse> {
    const r = await apiClient.get<Record<string, unknown>>(
      `/api/v1/pipeline/tasks/${encodeURIComponent(runId)}`,
    );
    return toCamelCase<PipelineTaskResponse>(r.data);
  },
};
