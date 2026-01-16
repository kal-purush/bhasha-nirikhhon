import * as userService from "../modules/user/user.service";
// import * as paymentService from "../modules/payment/payment.service";

/**
 * Fetch user orders along with payment details.
 * @param authId - The authenticated user's ID.
 * @returns An object containing orders and their associated payments.
 */
export const getUserOrdersWithPayment = async (authId: string) => {
  // Fetch user orders
  const orders = await userService.getUserOrders(authId);

  // Fetch payment details for the orders
  //   const payments = await paymentService.getPaymentsByUserId(authId);

  return { orders };
};