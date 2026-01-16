
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';


const statisticalApi = createApi({
    reducerPath: 'statistical',
    tagTypes: ['Statistical'],
    baseQuery: fetchBaseQuery({
        baseUrl: import.meta.env.VITE_API_URL,
    }),
    endpoints: (builder) => ({
        statisticalOrders: builder.query({
            query: (query) => {
                let url = `/statistical/orders?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        SellingProducts: builder.query({
            query: (query) => {
                let url = `/statistical/products-sell?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        viewedProducts: builder.query({
            query: (query) => {
                let url = `/statistical/products-view?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        TotalSoldQuantity: builder.query({
            query: (query) => {
                let url = `/statistical/products-sold?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        TotalCreatedProducts: builder.query({
            query: (query) => {
                let url = `/statistical/products-total?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        getCountOrder: builder.query({
            query: (query) => {
                let url = `/statistical/order-count?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        getCountUser: builder.query({
            query: (query) => {
                let url = `/statistical/users?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        getCommentStatiscal: builder.query({
            query: (query) => {
                let url = `/statistical/comments?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
        getProductSell: builder.query({
            query: (query) => {
                let url = `/statistical/products-selling?year=${query.year}`;
                if (query.month) {
                    url += `&month=${query.month}`;
                } else if (query.month == null) {
                    return url;
                }
                return url;
            },
            providesTags: ['Statistical']
        }),
    })
});

export const {
    useStatisticalOrdersQuery,
    useSellingProductsQuery,
    useViewedProductsQuery,
    useTotalSoldQuantityQuery,
    useTotalCreatedProductsQuery,
    useGetCountOrderQuery,
    useGetCountUserQuery,
    useGetCommentStatiscalQuery,
    useGetProductSellQuery
} = statisticalApi;
export const statisticalReducer = statisticalApi.reducer;
export default statisticalApi