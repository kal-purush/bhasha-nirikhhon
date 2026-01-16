import { notification } from "antd";
import { useMutation } from "react-query";
import { useNavigate } from "react-router-dom";
import { apiFranquia } from "../../../config/apiFranquia";
import { useFranchisorAuth } from "../../../contexts/franchisorAuthContext";
import { queryClient } from "../../../services/queryClient";
import { QueryKeys } from "../queryKeys";
import { Franchise } from "./__interfaces/franchises.interface";

export const useDeleteFranchise = () => {
  const { headers } = useFranchisorAuth();
  const navigate = useNavigate();
  const mutation = useMutation<
    Franchise | null | undefined,
    unknown,
    { id: string }
  >({
    mutationFn: async (body) => {
      const response = await apiFranquia.delete(
        `/franchise/delete/${body.id}`,
        {
          headers: { ...headers },
        }
      );
      await queryClient.refetchQueries({
        queryKey: [QueryKeys.LIST_FRANCHISES],
      });
      return response.data;
    },
    mutationKey: QueryKeys.DELETE_FRANCHISE,
  });

  const { data, error, isLoading, mutate, reset, isSuccess } = mutation;

  if (isSuccess) {
    notification.success({
      message: "Franquia deletada com sucesso!",
      props: { "data-testid": "success" },
    });
    reset();
    navigate(-1);
  }
  if (error) {
    notification.error({
      message: "Não foi possível deletar a franquia.",
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