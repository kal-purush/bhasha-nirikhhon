'use client';

import { IContributor } from '@/interfaces/entities/IContributor';
import { httpClient } from '@/lib/http';
import { useInfiniteQuery } from '@tanstack/react-query';

export default function useInfiniteContributors() {
  return useInfiniteQuery({
    queryKey: ['contributors'],
    queryFn: ({ pageParam }) => fetchPage(pageParam),
    initialPageParam: 1,
    getNextPageParam: page => page.nextPage,
    getPreviousPageParam: page => page.prevPage,
  });
}

async function fetchPage(page: number) {
  const response = await httpClient.get<IContributor[]>(
    `https://api.github.com/repos/angular/angular/contributors?page=${page}&per_page=25`,
    {
      headers: {
        Accept: 'application/vnd.github.v3+json',
      },
    }
  );

  const paginationLinks = parseLinkHeader(response.headers.link);

  return {
    data: response.data,
    nextPage: paginationLinks.next ?? null,
    prevPage: paginationLinks.prev ?? null,
    firstPage: paginationLinks.first ?? null,
    lastPage: paginationLinks.last ?? null,
  };
}

function parseLinkHeader(linkHeader: string) {
  if (!linkHeader) return {};

  const links: Record<string, number> = {};
  const linkParts = linkHeader.split(',');

  linkParts.forEach(part => {
    const match = part.match(/<([^>]+)>;\s*rel="([^"]+)"/);
    if (match) {
      const url = match[1];
      const rel = match[2];
      const urlObj = new URL(url);
      const page = Number(urlObj.searchParams.get('page'));
      links[rel] = page;
    }
  });

  return links;
}