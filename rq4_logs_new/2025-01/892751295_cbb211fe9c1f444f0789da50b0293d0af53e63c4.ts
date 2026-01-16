import {get, post, put} from 'sr/utils/axios/index'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'
import {WorkOrderResponse} from './fetchWorkOrder'

export interface CleanerFavWorkorderFilters {
  limit?: number
  page?: number
  cleaner_id?: string
}
interface UpdatedWorkorderRespone extends Omit<WorkOrderResponse, 'id'> {
  _id: string
}

export interface CleanerFavWorkorder {
  cleanner_id: string
  workorder_ids: UpdatedWorkorderRespone[]
  createdAt: string
  updatedAt: string
  id: string
}
export type CleanerFavWorkorderApiResponse = {
  success: boolean
  data: CleanerFavWorkorder
}

const filterPayload = (payload: CleanerFavWorkorderFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchCleanerFavWorkorder = async (
  payload: CleanerFavWorkorderFilters
): Promise<CleanerFavWorkorderApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<CleanerFavWorkorderApiResponse>(
      `/cleaner/favoriteworkorders`,
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

interface CleanerFavWorkorderVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createCleanerFavWorkorder = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/cleaner/favoriteworkorders `, payload)
    if (res.success === true) {
      toast.success('Cleaner FavWorkorder Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateCleanerFavWorkorder = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerFavWorkorderVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerFavWorkorderVariables>({
    mutationFn: async ({payload, onSuccess}) => createCleanerFavWorkorder(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerFavWorkorder'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateCleanerFavWorkorder = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/cleaner/favoriteworkorders`, payload)
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
export const useUpdateCleanerFavWorkorder = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  CleanerFavWorkorderVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, CleanerFavWorkorderVariables>({
    mutationFn: async ({payload, onSuccess}: CleanerFavWorkorderVariables) =>
      updateCleanerFavWorkorder(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['cleanerFavWorkorder'] as InvalidateQueryFilters)
      toast.success('Cleaner FavWorkorder Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}