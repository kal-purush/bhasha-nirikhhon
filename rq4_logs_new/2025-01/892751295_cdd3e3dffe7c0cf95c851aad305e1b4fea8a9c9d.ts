import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface CleanerPreferenceFilters {
  limit?: number
  page?: number
  cleaner_id?: string
}

export interface CleanerPreference {
  cleaner_id: string
  radius: number
  shift_type: string
  min_hours: number
  max_hours: number
  createdAt: string
  updatedAt: string
  id: string
}
export type CleanerPreferenceApiResponse = JmmApiResponse<CleanerPreference[]>
type SingleCleanerPreferenceApiResponse = JmmApiResponse<CleanerPreference>

const filterPayload = (payload: CleanerPreferenceFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCleanerPreference = async (
  payload: CleanerPreferenceFilters
): Promise<CleanerPreferenceApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CleanerPreferenceApiResponse>(`/cleaner/preference`, filteredPayload)

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

export const fetchSingleCleanerPreference = async (id: string): Promise<CleanerPreference> => {
  try {
    const res = await get<SingleCleanerPreferenceApiResponse>(`/cleaner/preference`, {id})

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

interface CleanerPreferenceVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createCleanerPreference = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/cleaner/preference `, payload)
    if (res.success === true) {
      toast.success('Cleaner Preference Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateCleanerPreference = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerPreferenceVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerPreferenceVariables>({
    mutationFn: async ({payload, onSuccess}) => createCleanerPreference(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerPreference'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCleanerPreference = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/cleaner/preference`, payload)
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
export const useUpdateCleanerPreference = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerPreferenceVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerPreferenceVariables>({
    mutationFn: async ({payload, onSuccess}: CleanerPreferenceVariables) =>
      updateCleanerPreference(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerPreference'] as InvalidateQueryFilters)
      toast.success('Cleaner Preference Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}