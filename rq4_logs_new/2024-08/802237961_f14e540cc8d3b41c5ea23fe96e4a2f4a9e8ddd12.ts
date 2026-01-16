import { apiFranqueadora } from "@config/api";
import { notification } from "antd";
import cookies from "js-cookie";
import { useMutation } from "react-query";
import { useNavigate } from "react-router-dom";
import { useFranchisorAuth } from "../../../contexts/franchisorAuthContext";
import { queryClient } from "../../../services/queryClient";
import { QueryKeys } from "../queryKeys";

export const useCreateUser = () => {
  const { headers } = useFranchisorAuth();
  const navigate = useNavigate();
  const mutation = useMutation<any | null | undefined, unknown, any>({
    mutationFn: async (body) => {
      const response = await apiFranqueadora.post(`/user`, body, {
        headers: { ...headers },
      });
      await queryClient.refetchQueries({
        queryKey: [QueryKeys.LIST_USERS],
      });
      return response.data;
    },
    mutationKey: QueryKeys.CREATE_USER,
  });

  const { data, error, isLoading, mutate, reset, isSuccess } = mutation;

  if (isSuccess) {
    notification.success({ message: "Usuário criado com sucesso!" });
    notification.info({
      message: "A sua senha de terminal.",
      description: `Armazene sua senha de terminal: ${data?.terminal_password}`,
      duration: 5000,
    });
    reset();
    cookies.remove("create_user");
    navigate("/usuários");
  }
  if (error) {
    notification.error({
      message: "Não foi possível criar o usuário.",
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