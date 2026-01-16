import { useCallback, useRef, useState } from "react";

import { useIntersectionObserver } from "@/hooks";
import { ApolloClient } from "@/utils";

import type { ApolloError, OperationVariables } from "@apollo/client";
import type { TQuery } from "@/queries/AnimeQueries";
import type { TUseIntersectionObserverReturn } from "@/hooks/useIntersectionObserver";

type TUseInfiniteQueryRefetchProps = {
  variables?: OperationVariables;
  resetPagination?: boolean;
};
type TUseInfiniteQueryRefetch = (options?: TUseInfiniteQueryRefetchProps) => Promise<void>;
type TUseInfiniteQueryProps = {
  variables: OperationVariables;
  debug?: boolean;
  onRefetch: (data: unknown) => void;
};
type TUseInfiniteQueryReturn = [
  observer: TUseIntersectionObserverReturn,
  response: {
    loading: boolean;
    error: ApolloError | null;
    hasNextPage: boolean;
    refetch: TUseInfiniteQueryRefetch;
  }
];
type TUseInfiniteQuery = (query: TQuery, options: TUseInfiniteQueryProps) => TUseInfiniteQueryReturn;

const useInfiniteQuery: TUseInfiniteQuery = (query, { variables, onRefetch }) => {
  const page = useRef(1);
  const hasNextPage = useRef(true);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApolloError | null>(null);

  const refetch: TUseInfiniteQueryRefetch = useCallback(
    async (options) => {
      if (options?.resetPagination) {
        page.current = 1;
      }

      setIsLoading(true);

      const queryVariables = {
        // Hook variables, can be overwritten by refetch variables - less priority
        ...variables,
        // Query variables - more priority
        ...options?.variables,
        // Pagination variables, cannot be overwritten - most priority
        page: page.current,
      };

      const { data, error } = await ApolloClient.query({
        query,
        variables: queryVariables,
      });

      if (error) {
        setError(error);
        setIsLoading(false);
        return;
      }

      onRefetch(data);

      hasNextPage.current = data.Page.pageInfo.hasNextPage;

      page.current += 1;

      setIsLoading(false);
    },
    [onRefetch, query, variables]
  );

  const observer = useIntersectionObserver({
    rootMargin: "100px",
    threshold: 0,
    canIntersect: !isLoading && !error,
    onIntersection: refetch,
  });

  return [observer, { loading: isLoading, error, hasNextPage: hasNextPage.current, refetch }];
};

export default useInfiniteQuery;