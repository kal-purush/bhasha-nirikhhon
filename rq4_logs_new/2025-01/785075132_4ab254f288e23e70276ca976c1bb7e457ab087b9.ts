import { Asset } from '@hike/types';
import { UseMutationOptions, useMutation } from '@tanstack/react-query';
import { rushWorkbench } from '../../api/workbench.service';
import { HikeError } from '../../errors/HikeError';

interface RushWorkbenchContext {
  workbenchId: string;
}

export const useRushWorkbench = (options?: UseMutationOptions<Asset[], HikeError<null>, RushWorkbenchContext>) =>
  useMutation({
    mutationKey: ['rushWorkbench'],
    mutationFn: async ({ workbenchId }: RushWorkbenchContext) => await rushWorkbench(workbenchId),
    ...options
  });