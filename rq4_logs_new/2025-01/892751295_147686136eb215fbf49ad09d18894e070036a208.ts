import {get, post, put} from 'sr/utils/axios/index'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface IndividualFavCleanerFilters {
  limit?: number
  page?: number
  individual_id?: string
}

export interface IndividualFavCleaner {
  individual_id: string
  cleaner_ids: Record<string, any>[]
  createdAt: string
  updatedAt: string
  id: string
}
export interface IndividualFavCleanerApiResponse {
  success: boolean
  data: IndividualFavCleaner
}

const filterPayload = (payload: IndividualFavCleanerFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchIndividualFavCleaner = async (
  payload: IndividualFavCleanerFilters
): Promise<IndividualFavCleanerApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<IndividualFavCleanerApiResponse>(
      `/individual/favcleaners`,
      filteredPayload
    )

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

interface IndividualFavCleanerVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createIndividualFavCleaner = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/individual/favcleaners`, payload)
    if (res.success === true) {
      toast.success('Individual FavCleaner Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateIndividualFavCleaner = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualFavCleanerVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualFavCleanerVariables>({
    mutationFn: async ({payload, onSuccess}) => createIndividualFavCleaner(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualFavCleaner'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateIndividualFavCleaner = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/individual/favcleaners`, payload)
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
export const useUpdateIndividualFavCleaner = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  IndividualFavCleanerVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, IndividualFavCleanerVariables>({
    mutationFn: async ({payload, onSuccess}: IndividualFavCleanerVariables) =>
      updateIndividualFavCleaner(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['individualFavCleaner'] as InvalidateQueryFilters)
      toast.success('Individual FavCleaner Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}