import { useMutation, UseMutationOptions } from "@tanstack/react-query";

import { deleteCart } from "@/lib/api/cart";
import { ResponseErr, ResponseSuccess } from "@/types/api";

interface DeleteCartParams {
  product_id: number;
}

interface DeleteCartResponse {
  success: boolean;
  message: string;
}

export const useDeleteCart = (
  options?: UseMutationOptions<
    ResponseSuccess<DeleteCartResponse>,
    ResponseErr,
    DeleteCartParams
  >,
) => {
  return useMutation({
    mutationKey: ["delete-cart"],
    mutationFn: (data: DeleteCartParams) => deleteCart(data.product_id),
    ...options,
  });
};