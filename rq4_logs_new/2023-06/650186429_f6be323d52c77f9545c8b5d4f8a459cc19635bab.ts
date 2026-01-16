import { Response } from "<@>/types/response";
import { getCookie } from "<@>/utils/cookie";
import { buildSelectors } from "@reduxjs/toolkit/dist/query/core/buildSelectors";
import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/dist/query/react";
import { url } from "inspector";

export const resourceApiSlice = createApi({
  reducerPath: "resource",
  baseQuery: fetchBaseQuery({
    baseUrl: "https://a2sv-community-portal-api.onrender.com/api",
    prepareHeaders: (headers) => {
      const token = getCookie("token");
      if (token) {
        headers.set("authorization", `bearer ${token}`);
      }
      return headers;
    },
  }),
  tagTypes: ["Resource", "Resources"],
  endpoints(builder) {
    return {
      getResource: builder.query({
        query: (id) => `/resource/${id}`,
        providesTags: ["Resource"],
      }),
      getResources: builder.query<Response, any>({
        query: () => "/resource",
        providesTags: ["Resources"],
      }),
      createResource: builder.mutation({
        query: (Resource) => {
          return { url: "/resource", method: "POST", body: Resource };
        },
        invalidatesTags: ["Resources", "Resource"],
      }),
      createTopic: builder.mutation({
        query: (Topic) => {
          return { url: "/resource/topic", method: "POST", body: Topic };
        },
        invalidatesTags: ["Resources"],
      }),
      editTopic: builder.mutation({
        query: (Topic) => {
          return { url: "/resource/topic", method: "PUT", body: Topic };
        },
        invalidatesTags: ["Resources"],
      }),
      editResource: builder.mutation({
        query: (Resource) => {
          return { url: "/resource", method: "PUT", body: Resource };
        },
        invalidatesTags: ["Resources", "Resource"],
      }),
      deleteResource: builder.mutation({
        query: (id) => {
          return { url: `/resource/${id}`, method: "DELETE" };
        },
        invalidatesTags: ["Resources"],
      }),
    };
  },
});

export const {
  useDeleteResourceMutation,
  useGetResourceQuery,
  useGetResourcesQuery,
  useEditResourceMutation,
  useCreateResourceMutation,
  useCreateTopicMutation,
  useEditTopicMutation,
} = resourceApiSlice;