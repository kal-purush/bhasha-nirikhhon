import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface CleanerJobTypeFilters {
  limit?: number
  page?: number
  cleaner_id?: string
  rate?: number
}

export interface CleanerJobType {
  cleaner_id: Record<string, any>
  job_type: string[]
  rate: number
  additional_information: string

  createdAt: string
  updatedAt: string
  id: string
}
export type CleanerJobTypeApiResponse = JmmApiResponse<CleanerJobType[]>
type SingleCleanerJobTypeApiResponse = JmmApiResponse<CleanerJobType>

const filterPayload = (payload: CleanerJobTypeFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCleanerJobType = async (
  payload: CleanerJobTypeFilters
): Promise<CleanerJobTypeApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CleanerJobTypeApiResponse>(`/cleaner/jobtypes`, filteredPayload)

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

export const fetchSingleCleanerJobType = async (id: string): Promise<CleanerJobType> => {
  try {
    const res = await get<SingleCleanerJobTypeApiResponse>(`/cleaner/jobtypes`, {id})

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

interface CleanerJobTypeVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createCleanerJobType = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/cleaner/jobtype`, payload)
    if (res.success === true) {
      toast.success('Cleaner JobType Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateCleanerJobType = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerJobTypeVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerJobTypeVariables>({
    mutationFn: async ({payload, onSuccess}) => createCleanerJobType(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerJobType'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCleanerJobType = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/cleaner/jobtype`, payload)
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
export const useUpdateCleanerJobType = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerJobTypeVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerJobTypeVariables>({
    mutationFn: async ({payload, onSuccess}: CleanerJobTypeVariables) =>
      updateCleanerJobType(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerJobType'] as InvalidateQueryFilters)
      toast.success('Cleaner JobType Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}