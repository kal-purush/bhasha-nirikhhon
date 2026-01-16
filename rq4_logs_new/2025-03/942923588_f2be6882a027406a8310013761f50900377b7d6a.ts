'use client';

import { IRepository } from '@/interfaces/entities/IRepository';
import { fetchPage } from '@/lib/utils';
import { useInfiniteQuery } from '@tanstack/react-query';

export default function useInfiniteRepos(tag: string) {
  return useInfiniteQuery({
    queryKey: [`repositories-${tag}`],
    queryFn: ({ pageParam }) =>
      fetchPage<IRepository[]>(
        `https://api.github.com/users/${tag}/repos?page=${pageParam}&per_page=15&sort=updated&direction=desc`
      ),
    initialPageParam: 1,
    getNextPageParam: page => page.nextPage,
    getPreviousPageParam: page => page.prevPage,
    retry: 5,
    retryDelay: 3000,
    retryOnMount: false,
  });
}