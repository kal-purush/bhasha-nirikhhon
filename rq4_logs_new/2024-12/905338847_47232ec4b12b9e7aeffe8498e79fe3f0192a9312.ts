import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";
import { Item } from "../types/types";

export const api = createApi({
  reducerPath: "api",
  baseQuery: fetchBaseQuery({ baseUrl: "http://localhost:8000/" }),
  endpoints: (builder) => ({
    getSaleProducts: builder.query<Item[], void>({
      query: () => "sale",
    }),

    getBestProducts: builder.query<Item[], void>({
      query: () => "best",
    }),

    getPopularProducts: builder.query<Item[], void>({
      query: () => "popular",
    }),

    getAllProducts: builder.query<Item[], void>({
      query: () => "all",
    }),
  }),
});

export const { useGetAllProductsQuery, useGetBestProductsQuery, useGetPopularProductsQuery, useGetSaleProductsQuery } =
  api;