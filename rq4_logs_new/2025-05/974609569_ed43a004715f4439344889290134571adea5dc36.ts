import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { useId } from "react";
import { client } from "@/lib/rpc";

interface AssignBadgeParams {
  badgeId: string;
  childId: string;
}

export function useAssignBadge() {
  const queryClient = useQueryClient();
  const toastId = useId();

  const mutation = useMutation({
    mutationFn: async (values: AssignBadgeParams) => {
      const res = await client.api.badges[":id"].$put({
        param: { id: values.badgeId },
        json: { childId: values.childId }
      });

      if (!res.ok) {
        const { message } = await res.json();

        throw new Error(message);
      }

      const data = await res.json();

      return data;
    },
    onMutate: () => {
      toast.loading("Assigning Badge...", { id: toastId });
    },
    onSuccess: () => {
      toast.success("Badge assigned successfully", { id: toastId });
      queryClient.invalidateQueries({ queryKey: ["badges"] });
    },
    onError: (error) => {
      toast.error(error.message || "Failed to assign badge", {
        id: toastId
      });
    }
  });

  return mutation;
}