import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface IndividualJobFilters {
  limit?: number
  page?: number
  individual_id?: string
  work_status?: string
  cleaner_id?: string
}

export interface IndividualJob {
  title: string
  streetAddress?: string
  unitorapt?: string
  description?: string
  individual_id: Record<string, any>
  schedule_date?: string
  work_status?: string
  createdAt: string
  updatedAt: string
  cleaner_id?: Record<string, any>
  start_time?: string
  id: string
}
export type IndividualJobApiResponse = JmmApiResponse<IndividualJob[]>
type SingleIndividualJobApiResponse = JmmApiResponse<IndividualJob>

const filterPayload = (payload: IndividualJobFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchIndividualJob = async (
  payload: IndividualJobFilters
): Promise<IndividualJobApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<IndividualJobApiResponse>(`/individual/job`, filteredPayload)

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

export const fetchSingleIndividualJob = async (
  id: string
): Promise<SingleIndividualJobApiResponse> => {
  try {
    const res = await get<SingleIndividualJobApiResponse>(`/individual/job`, {id})

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

interface IndividualJobVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createIndividualJob = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/individual/job`, payload)
    if (res.success === true) {
      toast.success('Individual Job Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateIndividualJob = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualJobVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualJobVariables>({
    mutationFn: async ({payload, onSuccess}) => createIndividualJob(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualJob'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateIndividualJob = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/individual/job`, payload)
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
export const useUpdateIndividualJob = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualJobVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualJobVariables>({
    mutationFn: async ({payload, onSuccess}: IndividualJobVariables) =>
      updateIndividualJob(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualJob'] as InvalidateQueryFilters)
      toast.success('Individual Job Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}