import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface IndividualChecklistFilters {
  limit?: number
  page?: number
  type?: string
  sub_type?: string
  individual_id?: string
}

export interface IndividualChecklist {
  name: string
  type: string
  sub_type: string
  individual_id: Record<string, any>
  createdAt: string
  updatedAt: string
  id: string
}
export type IndividualChecklistApiResponse = JmmApiResponse<IndividualChecklist[]>
type SingleIndividualChecklistApiResponse = JmmApiResponse<IndividualChecklist>

const filterPayload = (payload: IndividualChecklistFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchIndividualChecklist = async (
  payload: IndividualChecklistFilters
): Promise<IndividualChecklistApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<IndividualChecklistApiResponse>(`/individual/checklist`, filteredPayload)

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

export const fetchSingleIndividualChecklist = async (
  id: string
): Promise<SingleIndividualChecklistApiResponse> => {
  try {
    const res = await get<SingleIndividualChecklistApiResponse>(`/individual/checklist`, {id})

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

interface IndividualChecklistVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createIndividualChecklist = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/individual/checklist`, payload)
    if (res.success === true) {
      toast.success('Individual Checklist Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateIndividualChecklist = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualChecklistVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualChecklistVariables>({
    mutationFn: async ({payload, onSuccess}) => createIndividualChecklist(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualChecklist'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateIndividualChecklist = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/individual/checklist`, payload)
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
export const useUpdateIndividualChecklist = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualChecklistVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualChecklistVariables>({
    mutationFn: async ({payload, onSuccess}: IndividualChecklistVariables) =>
      updateIndividualChecklist(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualChecklist'] as InvalidateQueryFilters)
      toast.success('Individual Checklist Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}