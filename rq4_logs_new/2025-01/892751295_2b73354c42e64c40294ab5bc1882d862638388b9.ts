import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface CleanerMedicalFilters {
  limit?: number
  page?: number
  cleaner_id?: string
}

export interface CleanerMedical {
  cleaner_id: string
  condition: string
  since_when: string
  description: string
  createAt: string
  updatedAt: string
  id: string
}
export type CleanerMedicalApiResponse = JmmApiResponse<CleanerMedical[]>
type SingleCleanerMedicalApiResponse = JmmApiResponse<CleanerMedical>

const filterPayload = (payload: CleanerMedicalFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCleanerMedical = async (
  payload: CleanerMedicalFilters
): Promise<CleanerMedicalApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CleanerMedicalApiResponse>(`/cleaner/medicals`, filteredPayload)

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

export const fetchSingleCleanerMedical = async (id: string): Promise<CleanerMedical> => {
  try {
    const res = await get<SingleCleanerMedicalApiResponse>(`/cleaner/medicals`, {id})

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

interface CleanerMedicalVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createCleanerMedical = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/cleaner/medical `, payload)
    if (res.success === true) {
      toast.success('Cleaner Medical Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateCleanerMedical = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerMedicalVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerMedicalVariables>({
    mutationFn: async ({payload, onSuccess}) => createCleanerMedical(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerMedical'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCleanerMedical = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/cleaner/medical`, payload)
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
export const useUpdateCleanerMedical = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerMedicalVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerMedicalVariables>({
    mutationFn: async ({payload, onSuccess}: CleanerMedicalVariables) =>
      updateCleanerMedical(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerMedical'] as InvalidateQueryFilters)
      toast.success('Cleaner Medical Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}