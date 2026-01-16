import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface CleanerEmploymentFilters {
  limit?: number
  page?: number
  cleaner_id?: string
  currently_employed?: boolean
}

export interface CleanerEmployment {
  cleaner_id: string
  currently_employed?: boolean
  company_name?: string
  job_title?: string
  company_location?: string
  working_since_start?: string
  working_since_end?: string
  createdAt: string
  updatedAt: string
  id: string
}
export type CleanerEmploymentApiResponse = JmmApiResponse<CleanerEmployment[]>
type SingleCleanerEmploymentApiResponse = JmmApiResponse<CleanerEmployment>

const filterPayload = (payload: CleanerEmploymentFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCleanerEmployment = async (
  payload: CleanerEmploymentFilters
): Promise<CleanerEmploymentApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CleanerEmploymentApiResponse>(`/cleaner/employments`, filteredPayload)

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

export const fetchSingleCleanerEmployment = async (id: string): Promise<CleanerEmployment> => {
  try {
    const res = await get<SingleCleanerEmploymentApiResponse>(`/cleaner/employments`, {id})

    if (res.success === true && res.data) {
      return res.data // Return the fetched data
    } else {
      // Handle the case where results are not present
      throw new Error('No results found')
    }
  } catch (error) {
    // Throw the error to be handled by the caller
    throw new Error(`Failed to fetch : ${error instanceof Error ? error.message : 'Unknown error'}`)
  }
}

interface CleanerEmploymentVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createCleanerEmployment = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/cleaner/employments`, payload)
    if (res.success === true) {
      toast.success('Cleaner Employment Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateCleanerEmployment = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerEmploymentVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerEmploymentVariables>({
    mutationFn: async ({payload, onSuccess}) => createCleanerEmployment(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerEmployment'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCleanerEmployment = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/cleaner/employments`, payload)
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
export const useUpdateCleanerEmployment = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerEmploymentVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerEmploymentVariables>({
    mutationFn: async ({payload, onSuccess}: CleanerEmploymentVariables) =>
      updateCleanerEmployment(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerEmployment'] as InvalidateQueryFilters)
      toast.success('Cleaner Employment Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}