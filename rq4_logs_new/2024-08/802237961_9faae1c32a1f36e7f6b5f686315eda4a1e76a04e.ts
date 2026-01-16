import { apiPortalEvent } from "@config/apiPortalEvent";
import { notification } from "antd";
import { useMutation } from "react-query";
import { useFranchiseAuth } from "../../../../contexts/franchiseAuthContext";
import { ProductType } from "./_interfaces/products.interface";
import ResponseI from "@franchise/services/__interfaces/response.interface";
import { queryClient } from "../../../../services/queryClient";
import { QueryKeys } from "@franchise/services/queryKeys";

interface ActivateEventArgs {
  body: ProductType;
  id: string;
}

export const useActivateProduct = () => {
  const { headers } = useFranchiseAuth();
  const mutation = useMutation<
    ResponseI<ProductType> | null | undefined,
    unknown,
    ActivateEventArgs
  >({
    mutationFn: async ({ body, id }) => {
      const response = await apiPortalEvent.put(`/product/enable/${id}`, body, {
        headers: { ...headers },
      });
      await queryClient.refetchQueries({
        queryKey: [QueryKeys.LIST_PRODUCTS],
      });
      return response.data;
    },
    mutationKey: QueryKeys.ACTIVATE_PRODUCT,
  });

  const { data, error, isLoading, mutate, reset, isSuccess } = mutation;

  if (isSuccess) {
    notification.success({ message: "Produto habilitado com sucesso!" });
    reset();
  }
  if (error) {
    notification.error({
      message: "Não foi possível habilitar o produto.",
      description: (error as any)?.response?.data?.message,
    });
    reset();
  }

  return {
    data,
    error,
    isLoading,
    mutate,
    isSuccess,
    reset,
  };
};