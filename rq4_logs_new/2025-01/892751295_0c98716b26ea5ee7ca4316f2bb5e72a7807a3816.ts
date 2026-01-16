import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {
  useMutation,
  useQueryClient,
  UseMutationResult,
  InvalidateQueryFilters,
} from '@tanstack/react-query'
import {toast} from 'react-toastify'
import {WorkOrderResponse} from './fetchWorkOrder'
import {CleanerDetails} from './fetchCleaner'
export interface WorkoderApplicationFilters {
  limit?: number
  page?: number
  wordorder_id?: string
  cleaner_id?: string
  status?: string
}
interface ModifiedWorkorderResponse extends Omit<WorkOrderResponse, 'id'> {
  _id: string
}
interface ModifiedCleanerResponse extends Omit<CleanerDetails, 'id'> {
  _id: string
}

export interface WorkorderApplication {
  workorder_id: ModifiedWorkorderResponse
  cleaner_id: ModifiedCleanerResponse
  status: string
  createdAt: string
  updatedAt: string
  id: string
}

export type WorkorderApplicationApiResponse = JmmApiResponse<WorkorderApplication[]>
export type SingleWorkorderApplicationApiResponse = JmmApiResponse<WorkorderApplication>

const filterPayload = (payload: WorkoderApplicationFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}
export const fetchWorkorderApplications = async (
  payload: WorkoderApplicationFilters
): Promise<WorkorderApplicationApiResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<WorkorderApplicationApiResponse>(`/workorderapplication`, filteredPayload)

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
export const fetchSingleWorkorderApplication = async (
  id: string
): Promise<SingleWorkorderApplicationApiResponse> => {
  try {
    const res = await get<SingleWorkorderApplicationApiResponse>(`/workorderapplication`, {id})

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
interface WorkorderApplicationVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createWorkorderApplication = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/workorderapplication`, payload)
    if (res.success === true) {
      toast.success('WorkorderApplication Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateWorkorderApplication = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  WorkorderApplicationVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, WorkorderApplicationVariables>({
    mutationFn: async ({payload, onSuccess}) => createWorkorderApplication(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['workorderApplication'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateWorkorderApplication = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/workorderapplication`, payload)
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
export const useUpdateWorkorderApplication = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  WorkorderApplicationVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, WorkorderApplicationVariables>({
    mutationFn: async ({payload, onSuccess}: WorkorderApplicationVariables) =>
      updateWorkorderApplication(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['workorderApplication'] as InvalidateQueryFilters)
      toast.success('WorkorderApplication Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}