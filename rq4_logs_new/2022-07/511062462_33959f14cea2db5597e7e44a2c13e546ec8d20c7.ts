import { globalApi } from "./global.api";

import { CreateRequest, Request } from "../../types/request.types";

export const requestApi = globalApi.injectEndpoints({
  endpoints: (build) => ({
    createRequest: build.mutation<Request, CreateRequest>({
      query: (requestData) => {
        const token = localStorage.getItem("token");
        return {
          url: `request`,
          method: "POST",
          body: requestData,
          headers: { Authorization: `Bearer ${token}` },
        };
      },
    }),
    getRequestIsSentByUser: build.query<boolean, CreateRequest>({
      query: ({ userId, type }) => {
        const token = localStorage.getItem("token");
        return {
          url: `requestIsSent?userId=${userId}&type=${type}`,
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        };
      },
    }),
  }),
});

export const { useCreateRequestMutation, useGetRequestIsSentByUserQuery } =
  requestApi;