export interface Pagination {
  total: number;
  skip: number;
  count: number;
}

export interface ApiResponse<T> {
  data: T;
  pagination: Pagination;
}

export interface EmptyResponse {
  data: [];
  pagination: Pagination;
} 