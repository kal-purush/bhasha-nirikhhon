import type { ImportEvaluationsResponse } from '@hike/types';
import { QueryKey, UseQueryOptions, useQuery } from '@tanstack/react-query';
import { fetchEvaluationsImportStatus } from '../../api/evaluation.service';
import { HikeError } from '../../errors/HikeError';

export interface UseEvaluationsImportJobOptions
  extends Omit<
    UseQueryOptions<{ progress: number; data?: ImportEvaluationsResponse }, HikeError<null>>,
    'queryKey' | 'queryFn'
  > {
  id: string;
  queryKey?: QueryKey;
}

export const useEvaluationsImportJob = ({ id, queryKey = [], ...options }: UseEvaluationsImportJobOptions) =>
  useQuery({
    queryKey: ['evaluationsImportJob', id, queryKey],
    queryFn: async () => await fetchEvaluationsImportStatus(id),
    ...options
  });