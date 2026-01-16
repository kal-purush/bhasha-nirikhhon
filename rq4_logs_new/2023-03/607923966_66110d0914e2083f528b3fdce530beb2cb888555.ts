import { useInfiniteQuery, useQueryClient } from '@tanstack/react-query';
import { getCursorTodo } from '@client/api/todos';


export const useGetCursorTodo = () => {
  const {
    data,
    error,
    fetchNextPage,
    fetchPreviousPage,
    hasNextPage,
    isFetching,
    isFetchingNextPage,
    status
  } = useInfiniteQuery(
    ['allTodo'],
    async ({ pageParam = 1 }) => await getCursorTodo({ cursor: pageParam }),
    {
      getNextPageParam: (lastPage, pages) => lastPage.next,
      getPreviousPageParam: (firstPage, pages) => firstPage.prev
    }
  )

  const queryClient = useQueryClient()
  const removeData = (num: number) => {
    // TODO: any
    const res = queryClient.setQueryData(['allTodo'], (data: any) => ({
      pages: data.pages.slice(num),
      pageParams: data.pageParams.slice(num)
    }))
    return res
  }

  return {
    data,
    error,
    fetchNextPage,
    fetchPreviousPage,
    hasNextPage,
    isFetching,
    isFetchingNextPage,
    status,
    removeData
  }
}