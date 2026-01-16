import { notification } from "antd";
import cookies from "js-cookie";
import { useMutation } from "react-query";
import { useNavigate } from "react-router-dom";
import { apiFranquia } from "../../../config/apiFranquia";
import { useFranchiseAuth } from "../../../contexts/franchiseAuthContext";
import { queryClient } from "../../../services/queryClient";
import { QueryKeys } from "../queryKeys";
import { createClientI } from "@franchisor/services/clients/__interfaces/create_client.interface";

export const useCreateClient = () => {
  const { headers } = useFranchiseAuth();
  const navigate = useNavigate();
  const mutation = useMutation<
    any | null | undefined,
    unknown,
    createClientI
  >({
    mutationFn: async (body) => {
      const response = await apiFranquia.post(`/client`, body, {
        headers: { ...headers },
      });
      await queryClient.refetchQueries({
        queryKey: [QueryKeys.LIST_PROMOTERS],
      });
      return response.data;
    },
    mutationKey: QueryKeys.CREATE_CLIENT,
  });

  const { data, error, isLoading, mutate, reset, isSuccess } = mutation;

  if (isSuccess) {
    notification.success({ message: "Cliente criado com sucesso!" });
    notification.info({
      message: "A sua senha de terminal.",
      description: `Armazene sua senha de terminal: ${data?.terminal_password}`,
      duration: 5000,
    });
    reset();
    cookies.remove("create_client");
    navigate("/clientes");
  }
  if (error) {
    notification.error({
      message: "Não foi possível criar o cliente.",
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