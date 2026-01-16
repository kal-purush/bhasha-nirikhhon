import {get, post, put} from 'sr/utils/axios/index'
import {JmmApiResponse} from './contant'
import {Address} from './addressApi'
import {CleanerDetails} from './fetchCleaner'
import {CompanyResponse} from './fetchCompany'
import {toast} from 'react-toastify'
import {
  InvalidateQueryFilters,
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query'

interface UpdatedCleaner extends Omit<CleanerDetails, 'id'> {
  _id: string
}

interface UpdatedCompany extends Omit<CompanyResponse, 'id'> {
  _id: string
}

export interface ContractorDetails {
  cleaner_id: UpdatedCleaner
  first_name: string
  last_name: string
  mobile_number: string
  email?: string
  date_of_birth: string
  address: Address
  customer_location_ids: string[]
  company_id: UpdatedCompany
  status: string
  createdAt: string
  updatedAt: string
  id: string
}

export type FetchContractorResponse = JmmApiResponse<ContractorDetails[]>
export type FetchSingleContractorResponse = JmmApiResponse<ContractorDetails>

export interface ContractorListPayload {
  limit?: number
  page?: number
  sortBy?: string
  status?: string
  company_id?: string
  cleaner_id?: string
}

const filterPayload = (payload: ContractorListPayload) => {
  return Object.fromEntries(
    Object.entries(payload).filter(([_, value]) => value !== undefined && value !== null)
  )
}

export const fetchContractor = async (
  payload: ContractorListPayload
): Promise<FetchContractorResponse> => {
  const filteredPayload = filterPayload(payload)

  try {
    const res = await get<FetchContractorResponse>(`/contractor`, filteredPayload)

    if (res.success === true && res.data) {
      return res
    } else {
      throw new Error('No contractor found')
    }
  } catch (error) {
    throw new Error(
      `Failed to fetch contractor: ${error instanceof Error ? error.message : 'Unknown error'}`
    )
  }
}

export const fetchSingleContractor = async (id: string): Promise<FetchSingleContractorResponse> => {
  try {
    const res = await get<FetchSingleContractorResponse>(`/contractor`, {id})

    if (res.success === true && res.data) {
      return res
    } else {
      throw new Error('No contractor found')
    }
  } catch (error) {
    throw new Error(
      `Failed to fetch contractor: ${error instanceof Error ? error.message : 'Unknown error'}`
    )
  }
}
interface ContractorVariables {
  payload: Record<string, any>
  onSuccess: (action: string) => void
}
const createContractor = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await post<any>(`/contractor`, payload)
    if (res.success === true) {
      toast.success('Contractor Created Successfully')
      onSuccess('create')
      return true
    }
    throw new Error('Create failed')
  } catch (e: any) {
    throw new Error(e.message)
  }
}

// The useMutation hook with correct typing
export const useCreateContractor = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  ContractorVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, ContractorVariables>({
    mutationFn: async ({payload, onSuccess}) => createContractor(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['contractor'] as InvalidateQueryFilters)
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}

// Define the function with correct typing
const updateContractor = async (
  payload: Record<string, any>,
  onSuccess: (action: string) => void
): Promise<boolean> => {
  try {
    const res = await put<any>(`/contractor`, payload)
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
export const useUpdateContractor = (): UseMutationResult<
  boolean, // The type of the data returned on success
  Error, // The type of the error that could be thrown
  ContractorVariables // The type of the variables passed to the mutation
> => {
  const queryClient = useQueryClient()

  return useMutation<boolean, Error, ContractorVariables>({
    mutationFn: async ({payload, onSuccess}: ContractorVariables) =>
      updateContractor(payload, onSuccess),

    onSuccess: () => {
      queryClient.invalidateQueries(['contractor'] as InvalidateQueryFilters)
      toast.success('Contractor Updated Successfully')
    },
    onError: (error: Error) => {
      toast.error(error.message)
    },
  })
}