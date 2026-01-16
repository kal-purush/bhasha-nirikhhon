import { useId } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { CreateClassSchema } from "../schemas/create-class";
import { client } from "@/lib/rpc";

export function useCreateClass() {
  const queryClient = useQueryClient();
  const toastId = useId();

  const mutation = useMutation({
    mutationFn: async (values: CreateClassSchema) => {
      // Only send the fields that the client should provide
      const response = await client.api.classes.$post({
        json: values
      });

      if (!response.ok) {
        const { message } = await response.json();

        throw new Error(message);
      }

      return response;
    },
    onMutate() {
      toast.loading("Creating class...", { id: toastId });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["classes"] });
      toast.success("Class created successfully", { id: toastId });
    },
    onError: (error) => {
      toast.error("Failed to create class", {
        id: toastId,
        description: error.message
      });
    }
  });

  return mutation;
}