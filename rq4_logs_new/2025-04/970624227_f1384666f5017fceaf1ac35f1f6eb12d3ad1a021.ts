type TLaravelResponse<S = unknown, E = unknown> =
  | TLaravelSuccess<S>
  | TLaravelError<E>
  | undefined;

export type TLaravelSuccess<T = unknown> = {
  success: true;
  message: string;
  data: T;
};

export type TLaravelError<T extends Array<string> | unknown = unknown> = {
  success: false;
  message: string;
  errors?: T extends Array<string> ? { [k in T[number]]: string[] } : T;
};

export type TLaravelObject = {
  id: number;
  created_at: string;
  updated_at: string;
};

export type TLaravelPagination<T = unknown> = {
  current_page: number;
  data: T;
  first_page_url: string;
  from: number;
  last_page: number;
  last_page_url: string;
  links: { url: string | null; label: string; active: boolean }[];
  next_page_url: string | null;
  path: string;
  per_page: number;
  prev_page_url: string | null;
  to: number;
  total: number;
};

export type TODO = any;

export default TLaravelResponse;