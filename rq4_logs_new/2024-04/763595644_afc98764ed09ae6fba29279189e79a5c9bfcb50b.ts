import { useQueries } from '@tanstack/react-query';

import { getHierarchyDataById, getSuperiorHierarchies } from '~web/services';
import { useTutorialMode } from '~web/contexts';
import type { InitializationConfig } from './useInitializationConfig.types';
import type { QueryFunctionParams } from '~web/services';

export function useInitializationConfig<T>(
  serviceFn: (params: QueryFunctionParams<[string]>) => Promise<T>,
  { hash, id, ...initialData }: InitializationConfig<T>
) {
  const isTutorialMode = useTutorialMode();

  const [
    { data: hierarchy = initialData.hierarchy },
    { data: superiors = initialData.superiors },
    { data: config = initialData.config },
  ] = useQueries({
    queries: [
      {
        enabled: isTutorialMode,
        queryHash: `hierarchy-${hash}-${id}`,
        queryKey: [id, true],
        queryFn: getHierarchyDataById,
      },
      {
        enabled: isTutorialMode,
        queryHash: `superior-${hash}-${id}`,
        queryKey: [id, true],
        queryFn: getSuperiorHierarchies,
      },
      {
        enabled: isTutorialMode,
        queryHash: `data-${hash}-${id}`,
        queryKey: [id, true],
        queryFn: serviceFn,
      },
    ],
  });

  return { hierarchy, superiors, config };
}