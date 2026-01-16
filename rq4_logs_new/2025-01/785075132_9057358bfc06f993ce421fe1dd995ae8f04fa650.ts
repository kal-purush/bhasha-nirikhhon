import { ResetWorkbenchParams, Workbench } from '@hike/types';
import { UseMutationOptions, useMutation } from '@tanstack/react-query';
import { resetScans } from '../../api/workbench.service';
import { HikeError } from '../../errors/HikeError';

interface ResetWorkbenchContext {
  params: ResetWorkbenchParams;
  workbenchId: string;
}

export const useResetWorkbench = (options?: UseMutationOptions<Workbench, HikeError<null>, ResetWorkbenchContext>) =>
  useMutation({
    mutationKey: ['resetWorkbench'],
    mutationFn: async ({ workbenchId, params }: ResetWorkbenchContext) => await resetScans(workbenchId, params),
    ...options
  });