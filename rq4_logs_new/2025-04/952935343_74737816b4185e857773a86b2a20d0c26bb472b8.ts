import { useMutation, UseMutationOptions } from "@tanstack/react-query";

import { orderNow } from "@/lib/api/order";
import { ResponseErr, ResponseSuccess } from "@/types/api";

interface OrderNowParams {
  type: string;
  product_id: number;
  quantity: number;
  discount_code: string;
}

interface OrderNowResponse {
  success: boolean;
  message: string;
}

export const useOrderNow = (
  options?: UseMutationOptions<
    ResponseSuccess<OrderNowResponse>,
    ResponseErr,
    OrderNowParams
  >,
) => {
  return useMutation({
    mutationFn: ({
      type,
      product_id,
      quantity,
      discount_code,
    }: OrderNowParams) => orderNow(type, product_id, quantity, discount_code),
    ...options,
  });
};