import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'

type Character = {
  id: number
  name: string
  // status: string
  species: string
  // type: string
  gender: string
  // origin: {
  //   name: string
  //   url: string
  // }
  // location: {
  //   name: string
  //   url: string
  // }
  image: string
  // episode: string[]
  // url: string
  // created: string
}

export const characterApi = createApi({
  reducerPath: 'characterApi',
  baseQuery: fetchBaseQuery({ baseUrl: 'https://rickandmortyapi.com/api/' }),
  endpoints: (builder) => ({
    getCharacters: builder.query<Character[], null>({
      query: () => 'characters'
    }),
    getCharacterById: builder.query<Character, { id: number }>({
      query: ({ id }) => `character/${id}`
    })
  })
})

export const { useGetCharactersQuery, useGetCharacterByIdQuery } = characterApi