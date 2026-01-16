import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface IndividualTasklistFilters {
  limit?: number
  page?: number
  type?: string
  checklist_id?: string
  status?: string
  individual_id?: string
}

export interface IndividualTasklist {
  name: string
  type: string
  checklist_id: Record<string, any>
  individual_id: Record<string, any>
  description: string
  status: string
  createdAt: string
  updatedAt: string
  id: string
}
export type IndividualTasklistApiResponse = JmmApiResponse<IndividualTasklist[]>
type SingleIndividualTasklistApiResponse = JmmApiResponse<IndividualTasklist>

const filterPayload = (payload: IndividualTasklistFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchIndividualTasklist = async (
  payload: IndividualTasklistFilters
): Promise<IndividualTasklistApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<IndividualTasklistApiResponse>(`/individual/tasklist`, filteredPayload)

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

export const fetchSingleIndividualTasklist = async (
  id: string
): Promise<SingleIndividualTasklistApiResponse> => {
  try {
    const res = await get<SingleIndividualTasklistApiResponse>(`/individual/tasklist`, {id})

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

interface IndividualTasklistVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createIndividualTasklist = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/individual/tasklist`, payload)
    if (res.success === true) {
      toast.success('Individual Tasklist Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateIndividualTasklist = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualTasklistVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualTasklistVariables>({
    mutationFn: async ({payload, onSuccess}) => createIndividualTasklist(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualTasklist'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateIndividualTasklist = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/individual/tasklist`, payload)
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
export const useUpdateIndividualTasklist = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualTasklistVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualTasklistVariables>({
    mutationFn: async ({payload, onSuccess}: IndividualTasklistVariables) =>
      updateIndividualTasklist(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualTasklist'] as InvalidateQueryFilters)
      toast.success('Individual Tasklist Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}