import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

export interface NotesResponse {
  _id: string
  company_id: string
  applicant_id: string
  notes: string
  createdAt: string
  updatedAt: string
  __v: number
  id: string
}

export type FetchNotesResponse = JmmApiResponse<NotesResponse[]>
export type FetchSingleNotesResponse = JmmApiResponse<NotesResponse>

export interface NotesFilters {
  limit?: number
  page?: number
  company_id?: string
  applicant_id?: string
}

const filterPayload = (payload: NotesFilters) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}

export const fetchNotes = async (payload: NotesFilters): Promise<FetchNotesResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<FetchNotesResponse>(`/application/notes`, filteredPayload)

    if (res.success === true && res.data) {
      return res // Return the fetched data
    } else {
      throw new Error('No data found')
    }
  } catch (error) {
    throw new Error(`Failed to fetch: ${error instanceof Error ? error.message : 'Unknown error'}`)
  }
}
export const fetchSingleNote = async (id: string): Promise<FetchSingleNotesResponse> => {
  try {
    const res = await get<FetchSingleNotesResponse>(`/application/notes`, {id})

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
interface NotesVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createNote = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/application/notes`, payload)
    if (res.success === true) {
      toast.success('Note Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}
// The useMutation hook with correct typing
export const useCreateNote = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  NotesVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, NotesVariables>({
    mutationFn: async ({payload, onSuccess}) => createNote(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['notes'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateNote = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/application/notes`, payload)
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
export const useUpdateNote = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  NotesVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, NotesVariables>({
    mutationFn: async ({payload, onSuccess}: NotesVariables) => updateNote(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['notes'] as InvalidateQueryFilters)
      toast.success('Note Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}