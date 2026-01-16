import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface IndividualJobCategoryFilters {
  limit?: number
  page?: number
  individual_id?: string
}

export interface IndividualJobCategory {
  name: string
  icon_path: string
  createdAt: string
  updatedAt: string
  id: string
}
export type IndividualJobCategoryApiResponse = JmmApiResponse<IndividualJobCategory[]>
type SingleIndividualJobCategoryApiResponse = JmmApiResponse<IndividualJobCategory>

const filterPayload = (payload: IndividualJobCategoryFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchIndividualJobCategory = async (
  payload: IndividualJobCategoryFilters
): Promise<IndividualJobCategoryApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<IndividualJobCategoryApiResponse>(
      `/individual/job/category`,
      filteredPayload
    )

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

export const fetchSingleIndividualJobCategory = async (
  id: string
): Promise<SingleIndividualJobCategoryApiResponse> => {
  try {
    const res = await get<SingleIndividualJobCategoryApiResponse>(`/individual/job/category`, {id})

    if (res.success === true && res.data) {
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

interface IndividualJobCategoryVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createIndividualJobCategory = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/individual/job/category`, payload)
    if (res.success === true) {
      toast.success('Individual JobCategory Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateIndividualJobCategory = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualJobCategoryVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualJobCategoryVariables>({
    mutationFn: async ({payload, onSuccess}) => createIndividualJobCategory(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualJobCategory'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateIndividualJobCategory = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/individual/job/category`, payload)
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
export const useUpdateIndividualJobCategory = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualJobCategoryVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualJobCategoryVariables>({
    mutationFn: async ({payload, onSuccess}: IndividualJobCategoryVariables) =>
      updateIndividualJobCategory(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualJobCategory'] as InvalidateQueryFilters)
      toast.success('Individual JobCategory Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}