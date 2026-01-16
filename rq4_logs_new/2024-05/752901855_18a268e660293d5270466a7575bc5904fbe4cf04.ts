import { baseApi } from './baseApi'
import { SubscriptionHistoryResponse } from '@/types/subscription'

export const subscriptionApi = baseApi.injectEndpoints({
  endpoints: (builder) => ({
    getSubscriptions: builder.query<SubscriptionHistoryResponse[], void>({
      query: () => ({
        url: '/subscriptions/history/',
        method: 'GET',
      }),
      providesTags: (result) =>
        result
          ? [
              ...result.map(({ id }) => ({
                type: 'SubscriptionHistory' as const,
                id: id.toString(),
              })),
              'SubscriptionHistory',
            ]
          : ['SubscriptionHistory'],
    }),
  }),
})

export const { useGetSubscriptionsQuery } = subscriptionApi