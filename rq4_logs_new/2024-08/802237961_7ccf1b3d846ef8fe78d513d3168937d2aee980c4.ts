import { apiPortalEvent } from "@config/apiPortalEvent";
import { QueryKeys } from "@franchise/services/queryKeys";
import { notification } from "antd";
import { useMutation } from "react-query";
import { useNavigate } from "react-router-dom";
import { useFranchiseAuth } from "../../../../contexts/franchiseAuthContext";
import { queryClient } from "../../../../services/queryClient";
import { CreateProductType } from "./_interfaces/create_product.interface";

export const useCreateProduct = () => {
  const { headers } = useFranchiseAuth();
  const navigate = useNavigate();
  const mutation = useMutation<
    any | null | undefined,
    unknown,
    CreateProductType
  >({
    mutationFn: async (body) => {
      const response = await apiPortalEvent.post(`/product`, body, {
        headers: { ...headers },
      });
      await queryClient.refetchQueries({
        queryKey: [QueryKeys.LIST_PRODUCTS],
      });
      return response.data;
    },
    mutationKey: QueryKeys.CREATE_PRODUCT,
  });

  const { data, error, isLoading, mutate, reset, isSuccess } = mutation;

  if (isSuccess) {
    notification.success({ message: "Produto cadastrado com sucesso!" });
    reset();
    navigate("/fichas/produtos");
  }
  if (error) {
    notification.error({
      message: "Não foi possível cadastrar o produto.",
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