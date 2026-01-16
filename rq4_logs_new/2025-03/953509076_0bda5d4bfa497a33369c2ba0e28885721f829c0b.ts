import {
  useSuspenseInfiniteQuery,
  useSuspenseQuery,
} from '@tanstack/react-query';
import { QUERY_KEYS } from './queryKeys';
import { fetchCategories, fetchFaqs } from './faq';
import { CategoryType, SubCategoryType } from '@/types';

interface UseFaqListParams {
  category: CategoryType;
  subCategory: SubCategoryType;
  limit?: number;
  query?: string;
}

export function useFaqCategories(tab: 'CONSULT' | 'USAGE') {
  return useSuspenseQuery({
    queryKey: QUERY_KEYS.FAQ.CATEGORIES(tab),
    queryFn: () => fetchCategories(tab),
  });
}

export function useFaqList({
  category,
  subCategory = 'ALL',
  limit = 10,
}: UseFaqListParams) {
  return useSuspenseInfiniteQuery({
    queryKey: QUERY_KEYS.FAQ.LIST(category, subCategory),
    queryFn: ({ pageParam = 0 }) =>
      fetchFaqs(category, subCategory, pageParam, limit),
    initialPageParam: 0,
    getNextPageParam: (lastPage) => {
      const { offset, limit, totalRecord } = lastPage.pageInfo;
      if (offset + limit >= totalRecord) return undefined;
      return offset + limit;
    },
  });
}