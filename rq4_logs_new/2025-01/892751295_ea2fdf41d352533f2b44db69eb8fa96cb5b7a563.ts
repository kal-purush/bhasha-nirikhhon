import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {
  useMutation,
  useQueryClient,
  UseMutationResult,
  InvalidateQueryFilters,
} from '@tanstack/react-query'
import {toast} from 'react-toastify'
export interface CustomerLocationFilters {
  limit?: number
  page?: number
  company_id?: string
  customer_id?: string
  type?: string
}

export interface CustomerLocation {
  name: string
  customer_id: string
  company_id: string
  address?: Record<string, any>
  contacts: Record<string, any>[]
  type: string
  checklist_ids: string[]
  geofence_ids: string[]
  createdAt: string
  updatedAt: string
  id: string
}

export type CustomerLocationApiResponse = JmmApiResponse<CustomerLocation[]>
export type SingleCustomerLocationApiResponse = JmmApiResponse<CustomerLocation>

const filterPayload = (payload: CustomerLocationFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCustomerLocations = async (
  payload: CustomerLocationFilters
): Promise<CustomerLocationApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CustomerLocationApiResponse>(`/customer/location`, filteredPayload)

    if (res.success && res.data && res.data.length > 0) {
      return res // Return the fetched data
    } else {
      // Handle the case where results are not present
      throw new Error('No results found')
    }
  } catch (error) {
    // Throw the error to be handled by the caller
    throw new Error(`Failed to fetch : ${error instanceof Error ? error.message : 'Unknown error'}`)
  }
}
// export const fetchSingleCustomerLocation = async (
//   id: string
// ): Promise<SingleCustomerLocationApiResponse> => {
//   try {
//     const res = await get<SingleCustomerLocationApiResponse>(`/customer/location`, {id})

//     if (res.success && res.data) {
//       return res // Return the fetched data
//     } else {
//       // Handle the case where results are not present
//       throw new Error('No results found')
//     }
//   } catch (error) {
//     // Throw the error to be handled by the caller
//     throw new Error(`Failed to fetch : ${error instanceof Error ? error.message : 'Unknown error'}`)
//   }
// }
interface CustomerLocationVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}

const createCustomerLocation = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/customer/location`, payload)
    if (res.success === true) {
      toast.success('Customer Location Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}
// The useMutation hook with correct typing
export const useCreateCustomerLocation = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CustomerLocationVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CustomerLocationVariables>({
    mutationFn: async ({payload, onSuccess}) => createCustomerLocation(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['customerLocation'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCustomerLocation = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/customer/location`, payload)
    if (res.success === true) {
      onSuccess('update')
      return true
    }
    throw new Error('Update failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useUpdateCustomerLocation = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CustomerLocationVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CustomerLocationVariables>({
    mutationFn: async ({payload, onSuccess}: CustomerLocationVariables) =>
      updateCustomerLocation(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['customerLocation'] as InvalidateQueryFilters)
      toast.success('Customer Location Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}