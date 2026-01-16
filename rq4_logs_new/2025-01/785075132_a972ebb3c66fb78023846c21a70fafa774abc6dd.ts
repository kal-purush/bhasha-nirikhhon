import { Asset } from '@hike/types';
import { UseMutationOptions, useMutation } from '@tanstack/react-query';
import { approveWorkbench } from '../../api/workbench.service';
import { HikeError } from '../../errors/HikeError';

interface ApproveWorkbenchContext {
  workbenchId: string;
}

export const useApproveWorkbench = (options?: UseMutationOptions<Asset[], HikeError<null>, ApproveWorkbenchContext>) =>
  useMutation({
    mutationKey: ['approveWorkbench'],
    mutationFn: async ({ workbenchId }: ApproveWorkbenchContext) => await approveWorkbench(workbenchId),
    ...options
  });