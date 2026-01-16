import { MW_URL, PAGE_SIZE } from '@/shared/core/envs';
import { createUrlWithSearchParams } from '@/shared/utils/create-url-with-search-params';
import { mwGET } from '@/shared/utils/mw-get';

import { orgListQueryPageSchema } from '@/orgs/core/schemas';

export const getOrgList = async (
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