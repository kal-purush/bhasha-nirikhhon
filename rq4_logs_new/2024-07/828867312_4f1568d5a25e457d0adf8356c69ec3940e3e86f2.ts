import { useCallback, useEffect, useState } from 'react';
import { getAllNews } from '@/requests/requests';
import { setNewsList } from '@/store/newsSlice';
import { useAppDispatch } from '@/store/hooks';

const useGetNewsList = (pageNumber: number) => {
  const dispatch = useAppDispatch();
  const [error, setError] = useState<boolean>(false);

  const fetchData = useCallback(
    async (page: number) => {
      if (page) {
        try {
          const data = await getAllNews(page);
          if (error) {
            setError(false);
          }
          dispatch(setNewsList(data));
        } catch (e) {
          setError(true);
        }
      }
    },
    [dispatch, error],
  );

  useEffect(() => {
    fetchData(pageNumber);
  }, [fetchData, pageNumber]);

  return { fetchData, error };
};

export default useGetNewsList;