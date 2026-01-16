import {Prisma} from "@prisma/client";
import prisma from "@/app/_lib/primsa";
import BillFindManyArgs = Prisma.BillFindManyArgs;

const includePayments: Prisma.BillInclude = {
  paymentBills: {
    include: {
      payment: true
    }
  }
};

const billIncludesPayment = Prisma.validator<Prisma.BillDefaultArgs>()({
  include: includePayments
});

export type BillIncludePayment = Prisma.BillGetPayload<typeof billIncludesPayment>

export async function getBillsWithPaymentsByBookingID(booking_id: number, args?: BillFindManyArgs) {
  return prisma.bill.findMany({
    ...args,
    where: {
      ...args?.where,
      booking_id,
    },
    include: includePayments,
  });
}