import { useId } from "react";
import { toast } from "sonner";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { client } from "@/lib/rpc";
import { type BankDetailsUpsertSchemaT } from "@/features/nurseries/schemas/upsert-nursery-bank-details";

interface MutationParams {
  nurseryId: string;
  formData: BankDetailsUpsertSchemaT;
}

export const useAddNurseryBankDetails = () => {
  const queryClient = useQueryClient();
  const toastId = useId();

  const mutation = useMutation({
    mutationFn: async (values: MutationParams) => {
      const updatedResultRes = await client.api.nurseries.bank[":id"].$post({
        param: { id: values.nurseryId },
        json: values.formData
      });

      if (!updatedResultRes.ok) {
        const errorRes = await updatedResultRes.json();

        throw new Error(errorRes.message);
      }

      const updatedResult = await updatedResultRes.json();

      return updatedResult;
    },
    onMutate: () => {
      toast.loading("Updating nursery bank details...", { id: toastId });
    },
    onSuccess: () => {
      toast.success("Nursery bank details updated successfully !", {
        id: toastId
      });
      queryClient.invalidateQueries({ queryKey: ["bank-details"] });
    },
    onError: (error) => {
      toast.error(error.message || "Failed to update nursery", {
        id: toastId
      });
    }
  });

  return mutation;
};