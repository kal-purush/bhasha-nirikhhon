import { apiSlice } from "../api/apiSlice";

export const ngoApi = apiSlice.injectEndpoints({
  endpoints: (builder) => ({
    registerNgo: builder.mutation({
      query: (body) => ({
        url: "/Ngo/register",
        method: "POST",
        body,
      }),
    }),
  }),
});

export const { useRegisterNgoMutation } = ngoApi;