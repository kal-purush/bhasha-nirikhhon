import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";

export const gamesApi = createApi({
  reducerPath: "gamesApi",
  baseQuery: fetchBaseQuery({ baseUrl: "https://www.balldontlie.io/api/v1/" }),
  endpoints: (builder) => ({
    getAllPostseasonGames: builder.query({
      query: () => "games?per_page=100&seasons%5B%5D=2022&postseason=true",
    }),
  }),
});

export const { useGetAllPostseasonGamesQuery } = gamesApi;