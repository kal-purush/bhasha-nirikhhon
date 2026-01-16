import {
  Advisory,
  ApiPaginatedResult,
  ApiRequestParams,
} from "@app/api/models";
import { getAdvisories } from "@app/api/rest";
import { serializeRequestParamsForApi } from "@app/shared/hooks/table-controls";
import { useQuery } from "@tanstack/react-query";

export interface IAdvisoriesFetchState {
  result: ApiPaginatedResult<Advisory>;
  isFetching: boolean;
  fetchError: unknown;
  refetch: () => void;
}

export const AdvisoriesQueryKey = "advisories";

export const useFetchAdvisories = (params: ApiRequestParams = {}) => {
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: [
      AdvisoriesQueryKey,
      serializeRequestParamsForApi(params).toString(),
    ],
    queryFn: async () => await getAdvisories(params),
    onError: (error) => console.log("error, ", error),
    keepPreviousData: true,
  });
  return {
    result: data || { data: [], total: 0, params },
    isFetching: isLoading,
    fetchError: error,
    refetch,
  };
};