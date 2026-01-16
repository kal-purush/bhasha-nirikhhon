import { InfiniteData, useInfiniteQuery } from '@tanstack/react-query';

import { QUERY_STALETIME } from '@/shared/core/constants';
import { MW_URL, PAGE_SIZE } from '@/shared/core/envs';
import { createUrlWithSearchParams } from '@/shared/utils/create-url-with-search-params';
import { mwGET } from '@/shared/utils/mw-get';

import { OrgQueryKeys, orgQueryKeys } from '@/orgs/core/query-keys';
import { OrgListQueryPage, orgListQueryPageSchema } from '@/orgs/core/schemas';
import { useFiltersContext } from '@/filters/providers/filters-provider/context';

const getOrgList = async (
  page: number,
  searchParams: string | Record<string, string>,
) => {
  const url = createUrlWithSearchParams(
    `${MW_URL}/organizations/list?page=${page}&limit=${PAGE_SIZE}`,
    searchParams,
  );

  return mwGET({
    url,
    label: 'getOrgList',
    responseSchema: orgListQueryPageSchema,
    options: { next: { revalidate: 60 * 60 } },
  });
};

export const useOrgListQuery = () => {
  const { filterParamsString } = useFiltersContext();

  return useInfiniteQuery<
    OrgListQueryPage,
    Error,
    InfiniteData<OrgListQueryPage, number>,
    ReturnType<OrgQueryKeys['list']>,
    number
  >({
    queryKey: orgQueryKeys.list(filterParamsString),
    queryFn: async ({ pageParam }) => getOrgList(pageParam, filterParamsString),
    initialPageParam: 1,
    getNextPageParam: ({ page, data }) =>
      page > 0 && data.length > 0 ? page + 1 : undefined,
    staleTime: QUERY_STALETIME.DEFAULT,
  });
};