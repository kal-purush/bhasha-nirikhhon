import {
    PackageDetailRequest,
    PackageDetailResponse
  } from '@/types/package'
  import { baseApi } from './baseApi'
  
  export const packageApi = baseApi.injectEndpoints({
    endpoints: (builder) => ({
      getPackageDetail: builder.query<PackageDetailResponse, number>({
        query: (id) => ({
          url: `/packages/${id}/`,
          method: 'GET',
        }),
        providesTags: (result) => [{ type: 'Package', id: result?.id }],
      }),
    }),
  })
  
  export const {
    useGetPackageDetailQuery,
  } = packageApi
  