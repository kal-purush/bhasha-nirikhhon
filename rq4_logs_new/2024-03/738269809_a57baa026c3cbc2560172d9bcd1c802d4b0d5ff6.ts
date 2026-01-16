import { useQuery } from 'react-query';
import { useLocation } from 'react-router-dom';
import { axiosInstance } from '../api/axiosSetup';

export const useFetchData = <T>(pageType: string) => {
  const location = useLocation();

  const fetchData = async () => {
    const response = await axiosInstance.get<T>(location.search);

    return response.data;
  };

  const { data, error, isLoading } = useQuery({
    queryKey: [pageType, location],
    queryFn: fetchData,
    staleTime: Infinity,
  });

  return { data, error, isLoading };
};