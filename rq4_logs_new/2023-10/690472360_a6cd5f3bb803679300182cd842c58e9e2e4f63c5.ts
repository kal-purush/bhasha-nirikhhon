import { baseUrl } from '../../constants';

export type TGetCaseDetilsParams = {
  limit?: number;
  sortKey?: string;
  orderBy?: 'asc' | 'desc';
};

export function getCaseDetails({
  limit,
  sortKey,
  orderBy,
}: TGetCaseDetilsParams): Promise<any> {
  const params = {
    limit: limit ?? 5,
    sort_key: sortKey ?? 'created_at',
    order_by: orderBy ?? 'desc',
  };
  // const queryParams: string = objectToQueryParams<TGetCaseDetilsParams>(params);
  return fetch(`${baseUrl}/api/v1/case`, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(params),
  })
    .then((response) => response.json())
    .then((response) => {
      if (response.error) {
        return Promise.reject(response.message);
      }
      return response;
    })
    .then((response) => response.data);
}