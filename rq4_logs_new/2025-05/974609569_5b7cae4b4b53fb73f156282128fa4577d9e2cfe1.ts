import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { useId } from "react";

import { client } from "@/lib/rpc";
import { type AssignToClassSchema } from "@/features/children/schemas/assign-to-class";

interface MutationParams {
  id: string;
  values: AssignToClassSchema;
}

export function useAssignClass() {
  const queryClient = useQueryClient();
  const toastId = useId();

  const mutation = useMutation({
    mutationFn: async (values: MutationParams) => {
      const res = await client.api.children.assign[":id"].$put({
        json: values.values,
        param: { id: values.id }
      });

      if (!res.ok) {
        const { message } = await res.json();

        throw new Error(message);
      }

      const data = await res.json();

      return data;
    },
    onMutate: () => {
      toast.loading("Assigning to class...", { id: toastId });
    },
    onSuccess: () => {
      toast.success("Assigned to class successfully", { id: toastId });
      queryClient.invalidateQueries({ queryKey: ["children"] });
    },
    onError: (error) => {
      toast.error(error.message || "Failed to assign child", {
        id: toastId
      });
    }
  });

  return mutation;
}