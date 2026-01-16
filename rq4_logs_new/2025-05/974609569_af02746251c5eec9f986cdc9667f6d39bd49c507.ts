import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { useId } from "react";

import { client } from "@/lib/rpc";
import { type CreateChildSchema } from "@/features/children/schemas/create-child";

export function useCreateChild() {
  const queryClient = useQueryClient();
  const toastId = useId();

  const mutation = useMutation({
    mutationFn: async (values: CreateChildSchema) => {
      const res = await client.api.children.$post({
        json: values
      });

      if (!res.ok) {
        const { message } = await res.json();

        throw new Error(message);
      }

      const data = await res.json();

      return data;
    },
    onMutate: () => {
      toast.loading("Creating child...", { id: toastId });
    },
    onSuccess: () => {
      toast.success("Child created successfully", { id: toastId });
      queryClient.invalidateQueries({ queryKey: ["children"] });
    },
    onError: (error) => {
      toast.error(error.message || "Failed to create child", {
        id: toastId
      });
    }
  });

  return mutation;
}