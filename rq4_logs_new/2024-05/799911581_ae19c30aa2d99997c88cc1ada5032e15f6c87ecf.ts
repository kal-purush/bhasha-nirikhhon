import { InfiniteData, useInfiniteQuery } from '@tanstack/react-query';

import { QUERY_STALETIME } from '@/shared/core/constants';

import { ProjectQueryKeys, projectQueryKeys } from '@/projects/core/query-keys';
import { ProjectListQueryPage } from '@/projects/core/schemas';
import { getProjectList } from '@/projects/data/get-project-list';
import { useFiltersContext } from '@/filters/providers/filters-provider/context';

export const useProjectListQuery = () => {
  const { filterParamsString } = useFiltersContext();

  return useInfiniteQuery<
    ProjectListQueryPage,
    Error,
    InfiniteData<ProjectListQueryPage, number>,
    ReturnType<ProjectQueryKeys['list']>,
    number
  >({
    queryKey: projectQueryKeys.list(filterParamsString),
    queryFn: async ({ pageParam }) =>
      getProjectList({ page: pageParam, searchParams: filterParamsString }),
    initialPageParam: 1,
    getNextPageParam: ({ page, data }) =>
      page > 0 && data.length > 0 ? page + 1 : undefined,
    staleTime: QUERY_STALETIME.DEFAULT,
  });
};