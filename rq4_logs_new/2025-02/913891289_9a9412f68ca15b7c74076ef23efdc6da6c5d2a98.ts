import { PAYMENT_STATUS, PaymentStatus } from "@/types/order";

export const getPaymentStatus = (status: string): PaymentStatus | null => {
    switch (status) {
        case PAYMENT_STATUS.PAID:
            return PAYMENT_STATUS.PAID;
        case PAYMENT_STATUS.CHECKOUT:
            return PAYMENT_STATUS.CHECKOUT;
        case PAYMENT_STATUS.UNPAID:
            return PAYMENT_STATUS.UNPAID;
        default:
            return null;
    }
};