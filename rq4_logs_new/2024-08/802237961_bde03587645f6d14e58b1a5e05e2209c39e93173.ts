import { apiPortalEvent } from "@config/apiPortalEvent";
import { QueryKeys } from "@franchise/services/queryKeys";
import { notification } from "antd";
import { useMutation } from "react-query";
import { useNavigate } from "react-router-dom";
import { useFranchiseAuth } from "../../../contexts/franchiseAuthContext";
import { CreateMenuI } from "./__interfaces/create.menu.interface";
import { queryClient } from "../../../services/queryClient";

export const useCreateMenu = () => {
  const { headers } = useFranchiseAuth();
  const navigate = useNavigate();
  const mutation = useMutation<any | null | undefined, unknown, CreateMenuI>({
    mutationFn: async (body) => {
      const response = await apiPortalEvent.post(`/menu`, body, {
        headers: { ...headers },
      });
      await queryClient.refetchQueries({
        queryKey: [QueryKeys.LIST_MENUS],
      });
      return response.data;
    },
    mutationKey: QueryKeys.CREATE_MENU,
  });

  const { data, error, isLoading, mutate, reset, isSuccess } = mutation;

  if (isSuccess) {
    reset();
    navigate(`/fichas/cardápio/${data.id}/produtos`);
  }
  if (error) {
    notification.error({
      message: "Não foi possível cadastrar o cardápio.",
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