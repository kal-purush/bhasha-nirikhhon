// Base filter parameters that will be sent to the API
export interface BaseFilterParams {
  page: number
  pageSize: number
  sortField: string
  sortDirection?: 'asc' | 'desc'
  search?: string
}

// Base UI state for filters (internal representation)
export interface BaseFilterUIState {
  search: string
}

// Common pagination response structure
export interface PaginationResponse {
  currentPage: number
  totalPages: number
  pageSize: number
  totalCount: number
}

// Common API response structure
export interface ApiResponse<T> {
  success: boolean
  message?: string
  data?: T
  pagination?: PaginationResponse
  error?: any
}