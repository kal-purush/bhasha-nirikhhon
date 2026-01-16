// here I want to use RTK Query to fetch data from the server

import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';
import IResource from '<@>/types/resources/resourcesType';
import IResourceTopic from '<@>/types/resources/resourceListType';

export const resourcesApiSlice = createApi({
    reducerPath: 'resourcesApi',
    tagTypes: ['Resources'],
    baseQuery: fetchBaseQuery({
        baseUrl: 'https://a2sv-community-portal-api.onrender.com/api/Resources',
    }),
    endpoints(builder) {
        return {
            getResources: builder.query<IResource[], void>({
                query: () => ({
                    url: '/',
                    method: 'GET',
                }),
                providesTags: ['Resources'],
            }),
            getResourceTopics: builder.query<IResourceTopic[], void>({
                query: () => ({
                    url: '/Topics',
                    method: 'GET',
                }),
                providesTags: ['Resources'],
            }),
        };
    }
});


export const { useGetResourcesQuery } = resourcesApiSlice;