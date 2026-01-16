import { deliveryApi } from '@/redux/api/apiSlice';
import { PATH_CUSTOMER, PATH_LOGIN, PATH_PRODUCTS } from '@/constants';
import { IProduct } from '@/interfaces/IProduct';

export const productsApiSlice = deliveryApi.injectEndpoints({
  endpoints: (builder) => ({
    getProducts: builder.query<IProduct[], void>({
      query: () => ({
        url: `/${PATH_CUSTOMER}/${PATH_PRODUCTS}`,
      }),
      providesTags: ['Products'],
    }),
  }),
});

export const { useGetProductsQuery } = productsApiSlice;