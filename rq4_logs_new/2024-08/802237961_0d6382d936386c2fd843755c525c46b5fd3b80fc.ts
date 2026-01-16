import { apiPortalEvent } from "@config/apiPortalEvent";
import ResponseI from "@franchise/services/__interfaces/response.interface";
import { QueryKeys } from "@franchise/services/queryKeys";
import { useQuery } from "react-query";
import {
  ProductParams,
  ProductResponseSchema,
  ProductType,
} from "./_interfaces/products.interface";
import { useFranchiseAuth } from "../../../../contexts/franchiseAuthContext";

export const useListProducts = (params: ProductParams) => {
  const { headers } = useFranchiseAuth();
  const { data, error, isLoading, refetch } = useQuery<
    ResponseI<ProductType> | null | undefined
  >(
    [QueryKeys.LIST_PRODUCTS, params, headers],
    async () => {
      const response = await apiPortalEvent.get(`/product`, {
        headers: { ...headers },
        params: { orderBy: "created_at", orderDirection: "desc", ...params },
      });
      const parsedResponse = ProductResponseSchema.safeParse(response.data);
      if (!parsedResponse.success) {
        throw new Error(parsedResponse.error as any);
      }

      return parsedResponse.data;
    },
    {
      enabled: headers && headers["AuthToken"] ? true : false,
      refetchOnWindowFocus: true,
    }
  );

  return {
    data,
    error,
    isLoading,
    refetch,
  };
};