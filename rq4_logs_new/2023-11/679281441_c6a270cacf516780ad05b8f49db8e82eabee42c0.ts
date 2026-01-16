import { IBanner } from "@/interfaces/banner";
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';


const bannerApi = createApi({
    reducerPath: 'banners',
    tagTypes: ['Banner'],
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
        getBanner: builder.query<IBanner[], void>({
            query: () => '/banners',
            providesTags: ['Banner']
        }),
      
        getBannerById: builder.query<IBanner, number | string>({
            query: (id: any) => `/banners/${id}`,
            providesTags: ['Banner']
        }),
        
        addBanner: builder.mutation({
            query: (banner: IBanner) => ({
                url: '/banners',
                method: 'POST',
                body: banner
            }),
            invalidatesTags: ['Banner']
        }),
        removeBanner: builder.mutation<IBanner, string>({
            query: (id) => ({
                url: `/banners/${id}`,
                method: 'DELETE',
            }),
            invalidatesTags: ['Banner']
        }),
      
        updateBanner: builder.mutation({
            query: (banner: IBanner) => ({
                url: `/banners/${banner._id}`,
                method: 'PATCH',
                body: banner
            }),
            invalidatesTags: ['Banner']
        }),
     
    })
});

export const {
    useGetBannerQuery,
    useGetBannerByIdQuery,
    useAddBannerMutation,
    useRemoveBannerMutation,
    useUpdateBannerMutation,
    
} = bannerApi;
export const banerReducer = bannerApi.reducer;
export default bannerApi