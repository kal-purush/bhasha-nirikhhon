import { useQuery } from '@tanstack/react-query';
import { AxiosResponse } from 'axios';

import { api } from '@/constants';
import { file } from '@/models';
import { BaseTableQueryParams, Metadata } from '@/types';
import { axiosInstance } from '@/utils';

export type ListFilesResponse = {
  data: file.File[];
  metadata: Metadata;
};

const fetchFiles = async ({
  queryParams
}: {
  queryParams: BaseTableQueryParams;
}): Promise<ListFilesResponse> => {
  const { page, size, search } = queryParams;

  const params = {
    PageNumber: page,
    PageSize: size,
    ...(search && search != '' ? { SearchString: search } : {})
  };

  const response: ListFilesResponse = await axiosInstance.get(`/api/files`, {
    params
  });

  console.log('debug response', response);

  return response;
};

type UseListFilesParams = {
  queryParams: BaseTableQueryParams;
};

export const useListFiles = ({ queryParams }: UseListFilesParams) => {
  return useQuery<ListFilesResponse, Error>({
    queryKey: [api.FILE, queryParams],
    queryFn: () =>
      fetchFiles({
        queryParams
      }),
    keepPreviousData: true
  });
};