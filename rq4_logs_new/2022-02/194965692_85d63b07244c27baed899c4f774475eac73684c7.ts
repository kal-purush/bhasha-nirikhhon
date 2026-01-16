import qs from 'qs';
import apiClient from './apiClient';
import { CountsResult, CountResult, Attributes } from '../models/count';

export const countAPI = {
  getCount: async (url: string): Promise<CountsResult> => {
    const response = await apiClient.get<CountsResult>(
      `/api/counts?${qs.stringify(
        {
          filters: {
            url: {
              $eq: url,
            },
          },
        },
        {
          encodeValuesOnly: true,
        },
      )}`,
    );
    return response.data;
  },
  updateCount: async ({
    id,
    form,
  }: {
    id: number;
    form: Attributes;
  }): Promise<CountResult> => {
    const response = await apiClient.put<CountResult>(`/api/counts/${id}`, {
      data: form,
    });
    return response.data;
  },
  createCount: async (form: Attributes): Promise<CountResult> => {
    const response = await apiClient.post<CountResult>(`/api/counts`, {
      data: form,
    });
    return response.data;
  },
};