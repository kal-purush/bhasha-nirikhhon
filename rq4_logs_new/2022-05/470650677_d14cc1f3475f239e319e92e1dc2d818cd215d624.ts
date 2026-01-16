import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';

export const productsApi = createApi({
  baseQuery: fetchBaseQuery({ baseUrl: 'https://e6u9r2.deta.dev' }),
  reducerPath: 'productsApi',
  endpoints: build => ({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    getProducts: build.query<any, void>({
      query: () => '/euro',
    }),
  }),
});

export const { useGetProductsQuery } = productsApi;
export const { reducerPath, reducer, middleware } = productsApi;