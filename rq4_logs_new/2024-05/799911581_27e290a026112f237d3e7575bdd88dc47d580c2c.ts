import { useEffect } from 'react';
import { useInView } from 'react-intersection-observer';

import { useAtom } from 'jotai';

import { HREFS } from '@/shared/core/constants';
import { getQueryClient } from '@/shared/utils/get-query-client';

import { orgQueryKeys } from '@/orgs/core/query-keys';
import { initOrgAtom, orgTotalCountAtom } from '@/orgs/core/atoms';
import { initPathAtom } from '@/shared/core/atoms';
import { getOrgDetails } from '@/orgs/data/get-org-details';

import { useOrgListQuery } from './use-org-list-query';

export const useOrgList = () => {
  const queryClient = getQueryClient();

  const {
    data,
    error,
    fetchNextPage,
    hasNextPage,
    isSuccess,
    isPending,
    isFetching,
  } = useOrgListQuery();

  // Sync total count
  const [totalCount, setTotalCount] = useAtom(orgTotalCountAtom);
  useEffect(() => {
    const currentTotal = data?.pages[0].total ?? 0;
    if (data && currentTotal !== totalCount) {
      setTotalCount(currentTotal);
    }
  }, [data, setTotalCount, totalCount]);

  // Prefetch org details
  useEffect(() => {
    if (isSuccess && data) {
      const items = data.pages.flatMap((d) => d.data);
      for (const item of items) {
        const { orgId } = item;
        queryClient.prefetchQuery({
          queryKey: orgQueryKeys.details(orgId),
          queryFn: () => getOrgDetails(orgId),
        });
      }
    }
  }, [data, isSuccess, queryClient]);

  // Next page fetch on scroll
  const { ref: inViewRef } = useInView({
    threshold: 1,
    onChange: (inView) => {
      if (inView && !error && !isFetching) fetchNextPage();
    },
  });

  const [initPath] = useAtom(initPathAtom);
  const isOrgListSSR = initPath === HREFS.ORGS_PAGE;

  const [initOrg] = useAtom(initOrgAtom);
  const allOrgs = data?.pages.flatMap((d) => d.data) ?? [];

  // Dedupe init-card if not list-page ssr
  const orgs = !isOrgListSSR
    ? allOrgs.filter((d) => d.orgId !== initOrg?.orgId)
    : allOrgs;

  return {
    orgs,
    error,
    inViewRef,
    hasNextPage,
    isSuccess,
    isPending,
  };
};