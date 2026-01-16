import { Transaction } from '@/types/payment'
import { baseApi } from './baseApi'

export const paymentApi = baseApi.injectEndpoints({
  endpoints: (builder) => ({
    createTransaction: builder.mutation<
      { token: string; redirect_url: string },
      { package_id: number }
    >({
      query: (body) => ({
        url: '/payments/create/',
        method: 'POST',
        body,
      }),
    }),
    getTransaction: builder.query<
      { transaction_detail: Transaction },
      { order_id: string }
    >({
      query: ({ order_id }) => ({
        url: '/payments/create/',
        method: 'GET',
        params: {
          order_id,
        },
      }),
      providesTags: (result) => [
        { type: 'Transaction', id: result?.transaction_detail.id },
      ],
    }),
    getTransactionList: builder.query<Transaction, void>({
      query: () => ({
        url: '/payments/transactions/',
        method: 'GET',
      }),
      providesTags: ['Transaction'],
    }),
    getTransactionById: builder.query<Transaction, { id: string }>({
      query: ({ id }) => ({
        url: `/payments/transactions/${id}`,
        method: 'GET',
      }),
      providesTags: (result) => [{ type: 'Transaction', id: result?.id }],
    }),
  }),
})

export const {
  useCreateTransactionMutation,
  useLazyGetTransactionQuery,
  useGetTransactionListQuery,
  useLazyGetTransactionByIdQuery,
} = paymentApi