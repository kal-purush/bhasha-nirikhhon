import { toast } from "sonner";
import { useId } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { client } from "@/lib/rpc";
import { type CreateFeedbackSchema } from "../schemas/create-feedback";

interface MutationParams {
  values: CreateFeedbackSchema;
}

export function useCreateFeedback() {
  const queryClient = useQueryClient();
  const toastId = useId();

  const mutation = useMutation({
    mutationFn: async ({ values }: MutationParams) => {
      const res = await client.api.feedbacks.$post({
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
      toast.loading("Creating feedback...", { id: toastId });
    },
    onSuccess: () => {
      toast.success("Feedback created successfully", { id: toastId });
      queryClient.invalidateQueries({ queryKey: ["feedbacks"] });
    },
    onError: (error) => {
      toast.error(error.message || "Failed to create feedback", {
        id: toastId
      });
    }
  });

  return mutation;
}