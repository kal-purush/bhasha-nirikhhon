import { Response } from "<@>/types/response";
import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/dist/query/react";

export const notificationApiSlice = createApi({
  reducerPath: "notifications",
  baseQuery: fetchBaseQuery({
    baseUrl: "https://a2sv-community-portal-api.onrender.com/api",
    prepareHeaders: (headers, { getState }) => {
      const token = (getState() as any).auth.token;
      if (token) {
        headers.set("authorization", `bearer ${token}`);
      }
      return headers;
    },
  }),

  endpoints: (builder) => ({
    getNotifications: builder.query<Response, void>({
      query: () => ({
        url: "/Notifications",
      }),
    }),
  }),
});

export const { useGetNotificationsQuery } = notificationApiSlice;