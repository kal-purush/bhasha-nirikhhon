// Need to use the React-specific entry point to allow generating React hooks
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'

import { IPlaylist, IUser, TRootState } from '../interfacesAndTypes'

// Define a service using a base URL and expected endpoints
export const playlistsApi = createApi({
  reducerPath: 'playlistsApi',
  baseQuery: fetchBaseQuery({
    baseUrl: 'https://api.spotify.com/v1',
    prepareHeaders: (headers, { getState }) => {
      const state = getState() as TRootState
      const user = state.user as unknown as IUser
      if (user) {
        headers.set('Authorization', `Bearer ${user.accessToken}`)
      }
      return headers
    },
  }),
  endpoints: (builder) => ({
    getAllPlaylists: builder.query<IPlaylist[], string>({
      query: () => ({
        url: '/me/playlists',
        method: 'GET',
      }),
      transformResponse: (rawResult: { items: IPlaylist[] }, meta) => {
        // The return value for `transformResponse` must match `ResultType`
        return rawResult.items
      },
    }),
    getPlaylistById: builder.query<IPlaylist, string>({
      query: (id) => ({
        url: `playlists/${id}`,
        method: 'GET',
      }),
    }),
  }),
})

// Export hooks for usage in function components, which are
// auto-generated based on the defined endpoints
export const { useLazyGetAllPlaylistsQuery, useLazyGetPlaylistByIdQuery } =
  playlistsApi