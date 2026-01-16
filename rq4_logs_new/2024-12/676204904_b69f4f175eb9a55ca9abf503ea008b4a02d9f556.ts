import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';

const YANDEX_BASE_URL = 'https://oauth.yandex.ru/token';

export const yandexAuthApi = createApi({
  reducerPath: 'yandexAuthApi',
  baseQuery: fetchBaseQuery({ baseUrl: YANDEX_BASE_URL }),
  endpoints: (builder) => ({
    getYandexToken: builder.mutation({
      query: ({ clientId, clientSecret, code }) => ({
        url: '',
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${btoa(`${clientId}:${clientSecret}`)}`,
        },
        body: new URLSearchParams({
          grant_type: 'authorization_code',
          code,
        }).toString(),
      }),
    }),
  }),
});

export const { useGetYandexTokenMutation } = yandexAuthApi;