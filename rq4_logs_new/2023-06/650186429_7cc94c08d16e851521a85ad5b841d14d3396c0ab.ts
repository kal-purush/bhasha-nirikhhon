import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/dist/query/react";

export const contestApiSlice = createApi({
  reducerPath: "contest",
  baseQuery: fetchBaseQuery({
    baseUrl: "some",
  }),
  endpoints(builder) {
    return {
      getUpcomingContests: builder.query({
        query: () => "",
      }),
      getRecentContests: builder.query({
        query: () => "",
      }),
    };
  },
});

export const { useGetRecentContestsQuery, useGetUpcomingContestsQuery } =
  contestApiSlice;