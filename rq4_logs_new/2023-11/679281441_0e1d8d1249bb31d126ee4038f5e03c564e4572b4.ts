import { IContact } from '@/interfaces/contact';
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';

const contactApi = createApi({
    reducerPath: 'contact',
    tagTypes: ['Contact'],
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
        getAllContact: builder.query<IContact[], void>({
            query: () => '/contact',
            providesTags: ['Contact']
        }),
        getAllDeleteContact: builder.query<IContact[], void>({
            query: () => '/contact/delete',
            providesTags: ['Contact']
        })
        ,
        getContactById: builder.query<IContact, number | string>({
            query: (id: any) => `/contact/${id}`,
            providesTags: ['Contact']
        }),
        addContact: builder.mutation({
            query: (contact: IContact) => ({
                url: '/contact',
                method: 'POST',
                body: contact
            }),
            invalidatesTags: ['Contact']
        }),
        removeContact: builder.mutation<IContact, string>({
            query: (id) => ({
                url: `/contact/${id}`,
                method: 'DELETE',
            }),
            invalidatesTags: ['Contact']
        }),
        removeForceContact: builder.mutation<IContact, number | string>({
            query: (id) => ({
                url: `/contact/force/${id}`,
                method: 'DELETE',
            }),
            invalidatesTags: ['Contact']
        }),
        updateContact: builder.mutation({
            query: (contact: IContact) => ({
                url: `/contact/${contact._id}`,
                method: 'PATCH',
                body: contact
            }),
            invalidatesTags: ['Contact']
        }),
        restoreContact: builder.mutation({
            query: (id) => ({
                url: `/contact/restore/${id}`,
                method: 'PATCH'
            }),
            invalidatesTags: ['Contact']
        })
    })
});

export const {
    useGetAllContactQuery,
    useGetAllDeleteContactQuery,
    useGetContactByIdQuery,
    useAddContactMutation,
    useRemoveContactMutation,
    useRemoveForceContactMutation,
    useUpdateContactMutation,
    useRestoreContactMutation
} = contactApi;
export const contactReducer = contactApi.reducer;
export default contactApi