import { Announcement } from "<@>/types/admin/Announcement";
import { Response } from "<@>/types/response";
import { getCookie } from "<@>/utils/cookie";
import { buildSelectors } from "@reduxjs/toolkit/dist/query/core/buildSelectors";
import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/dist/query/react";
import { url } from "inspector";

export const announcementApiSlice = createApi({
  reducerPath: "announcement",
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
  tagTypes: ["Announcement", "Announcements"],
  endpoints(builder) {
    return {
      getAnnouncement: builder.query({
        query: (id) => `/blogs/${id}`,
        providesTags: ["Announcement"],
      }),
      getAnnouncements: builder.query<Response, any>({
        query: () => "/blogs",
        providesTags: ["Announcements"],
      }),
      createAnnouncement: builder.mutation({
        query: (announcement: Announcement) => {
          return { url: "/blogs", method: "POST", body: announcement };
        },
        invalidatesTags: ["Announcements"],
      }),
      updateAnnouncement: builder.mutation({
        query: (announcement) => {
          return { url: "/blogs", method: "PUT", body: announcement };
        },
        invalidatesTags: ["Announcements", "Announcement"],
      }),
      deleteAnnouncement: builder.mutation({
        query: (id) => {
          return { url: `/blogs/${id}`, method: "DELETE" };
        },
        invalidatesTags: ["Announcements"],
      }),
    };
  },
});

export const {
  useDeleteAnnouncementMutation,
  useGetAnnouncementQuery,
  useGetAnnouncementsQuery,
  useUpdateAnnouncementMutation,
  useCreateAnnouncementMutation,
} = announcementApiSlice;