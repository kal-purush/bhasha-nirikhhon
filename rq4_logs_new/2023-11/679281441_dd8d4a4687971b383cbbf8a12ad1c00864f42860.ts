
import { INew } from '@/interfaces/new';
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';


const newApi = createApi({
    reducerPath: 'news',
    tagTypes: ['News'],
    baseQuery: fetchBaseQuery({
        baseUrl: import.meta.env.VITE_API_URL,
        prepareHeaders: (headers) => {
            const storedData = localStorage.getItem('accessToken');
            if (storedData) {
                const { accessToken } = JSON.parse(storedData);
                if (accessToken) {
                    headers.set('Authorization', `Bearer ${accessToken}`);
                }
            }
            return headers;
        },

    }),
    endpoints: (builder) => ({
        getNews: builder.query<INew[], void>({
            query: () => '/news',
            providesTags: ['News']
        }),
        getAllDelete: builder.query<INew[], void>({
            query: () => '/news/delete',
            providesTags: ['News']
        })
        ,
        getNewById: builder.query<INew, number | string>({
            query: (id: any) => `/news/${id}`,
            providesTags: ['News']
        }),
        // 
        addNew: builder.mutation({
            query: (news: INew) => ({
                url: '/news',
                method: 'POST',
                body: news
            }),
            invalidatesTags: ['News']
        }),
        removeNew: builder.mutation<INew, string>({
            query: (id) => ({
                url: `/news/${id}`,
                method: 'DELETE',
            }),
            invalidatesTags: ['News']
        }),
        removeForceNew: builder.mutation<INew, number | string>({
            query: (id) => ({
                url: `/news/force/${id}`,
                method: 'DELETE',
            }),
            invalidatesTags: ['News']
        }),
        updateNew: builder.mutation({
            query: (news: INew) => ({
                url: `/news/${news._id}`,
                method: 'PATCH',
                body: news
            }),
            invalidatesTags: ['News']
        }),
        restoreNew: builder.mutation({
            query: (id) => ({
                url: `/news/restore/${id}`,
                method: 'PATCH'
            }),
            invalidatesTags: ['News']
        })
    })
});

export const {
    useGetNewsQuery,
    useGetNewByIdQuery,
    useGetAllDeleteQuery,
    useAddNewMutation,
    useRemoveNewMutation,
    useRemoveForceNewMutation,
    useUpdateNewMutation,
    useRestoreNewMutation
} = newApi;
export const newsReducer = newApi.reducer;
export default newApi