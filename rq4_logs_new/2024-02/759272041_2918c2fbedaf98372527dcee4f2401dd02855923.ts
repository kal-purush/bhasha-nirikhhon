import { useInfiniteQuery } from '@tanstack/react-query';

async function fetchInfiniteData({ pageParam = 1 }) {
  const limit = 10; // 한 번에 불러올 데이터 수
  const response = await fetch(`https://api.thecatapi.com/v1/images/search?limit=${limit}&page=${pageParam}`, {
    headers: { 'x-api-key': 'live_CGL7BtiNmm0aAub1mWrrHDEaRfoHNmd7z9OOTJZU8qcccVJ4GvsAAEqRKZOy6eRs' },
  });

  if (!response.ok) throw new Error('api error');

  const data = await response.json();
  const nextPage = data.length === limit ? pageParam + 1 : undefined; // 다음 페이지가 있는 경우
  return { data, nextPage };
};

export function useFetchInfiniteData() {
  return useInfiniteQuery({
    queryKey: ["catImages"],
    queryFn: fetchInfiniteData,
    initialPageParam: 1,
    getNextPageParam: (lastPageParam) => lastPageParam.nextPage,
  });
}