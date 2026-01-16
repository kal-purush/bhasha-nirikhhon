import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";
import { ClientError } from "graphql-request";
import { useState } from "react";
import useAuth from "./useAuth";

/**
 * SSRの場合はQueryClientをstate化する
 * @see https://tanstack.com/query/latest/docs/react/guides/ssr#using-hydration
 *
 * QueryClientをインスタンス化する際、共通エラーハンドリングを入れる
 * @see https://tkdodo.eu/blog/react-query-error-handling#the-global-callbacks
 *
 * @returns
 */
const useCustomQueryClient = () => {
  const { refreshIdToken } = useAuth();

  /**
   * 共通エラーハンドリング
   * @param error
   */
  const handleError = (error: unknown) => {
    // NOTE:
    // ステータスが401の場合はアクセストークン期限切れを想定、
    // トークンの更新を試みる
    if (error instanceof ClientError && error.response.status === 401) {
      console.warn("refreshIdToken @handleError");
      void (async () => await refreshIdToken())();
    }
  };

  const [customQueryClient] = useState(
    () =>
      new QueryClient({
        // https://tanstack.com/query/v4/docs/react/reference/QueryCache#global-callbacks
        queryCache: new QueryCache({
          onError: handleError,
        }),
        // https://tanstack.com/query/v4/docs/react/reference/MutationCache#global-callbacks
        mutationCache: new MutationCache({
          onError: handleError,
        }),
      })
  );

  return { customQueryClient };
};

export default useCustomQueryClient;