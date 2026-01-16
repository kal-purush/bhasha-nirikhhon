import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { CreatePaymentSchema } from "../schemas/create-payment";
import { client } from "@/lib/rpc";

interface CreatePaymentParams {
  values: CreatePaymentSchema;
}

export function useCreatePayment() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async ({ values }: CreatePaymentParams) => {
      const response = await client.api.payments.$post({
        json: values,
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Failed to create payment");
      }

      return response.json();
    },
    onSuccess: () => {
      // Invalidate queries to refresh any payment lists
      queryClient.invalidateQueries({ queryKey: ["payments"] });
    },
    onError: (error) => {
      console.error("Payment creation failed:", error);
      toast.error(
        "Failed to create payment: " +
          (error instanceof Error ? error.message : "Unknown error")
      );
    },
  });
}