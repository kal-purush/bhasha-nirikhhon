import axios from 'axios';
import { useQuery } from 'react-query';

export const useFetchListOfUrls = (urls: string[] | undefined) => {
  const promiseArray = urls?.map((url) => axios.get(url));

  const fetchListOfUrl = async () => {
    const response = await axios.all(promiseArray || []);

    return response.map((element) => element.data);
  };

  const { data, isError, isLoading, refetch } = useQuery({
    queryKey: [urls],
    queryFn: fetchListOfUrl,
  });

  if (!urls) {
    return {
      data: undefined,
      isError: false,
      isLoading: true,
      refetch: () => {},
    };
  }

  return { data, isError, isLoading, refetch };
};