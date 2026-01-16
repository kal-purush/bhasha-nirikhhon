import {get, post, put} from 'sr/utils/axios/index'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'
import {JobResponse} from './fetchJobs'

export interface CleanerFavJobFilters {
  limit?: number
  page?: number
  cleaner_id?: string
}
interface UpdatedJobRespone extends Omit<JobResponse, 'id'> {
  _id: string
}

export interface CleanerFavJob {
  cleanner_id: string
  job_ids: UpdatedJobRespone[]
  createdAt: string
  updatedAt: string
  id: string
}
export type CleanerFavJobApiResponse = {
  success: boolean
  data: CleanerFavJob
}

const filterPayload = (payload: CleanerFavJobFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCleanerFavJob = async (
  payload: CleanerFavJobFilters
): Promise<CleanerFavJobApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CleanerFavJobApiResponse>(`/cleaner/favoritejobs`, filteredPayload)

    if (res.success && res.data) {
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

interface CleanerFavJobVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createCleanerFavJob = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/cleaner/favoritejobs `, payload)
    if (res.success === true) {
      toast.success('Cleaner FavJob Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateCleanerFavJob = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerFavJobVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerFavJobVariables>({
    mutationFn: async ({payload, onSuccess}) => createCleanerFavJob(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerFavJob'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCleanerFavJob = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/cleaner/favoritejobs`, payload)
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
export const useUpdateCleanerFavJob = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerFavJobVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerFavJobVariables>({
    mutationFn: async ({payload, onSuccess}: CleanerFavJobVariables) =>
      updateCleanerFavJob(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerFavJob'] as InvalidateQueryFilters)
      toast.success('Cleaner FavJob Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}