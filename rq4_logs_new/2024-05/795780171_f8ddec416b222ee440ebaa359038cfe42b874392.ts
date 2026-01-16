"use server";

import { prisma } from "@/lib/prisma";

interface updatePaymentStatusProps {
  memberId: string;
  paymentStatus: boolean;
}

export const updatePaymentStatus = async ({
  memberId,
  paymentStatus,
}: updatePaymentStatusProps) => {
  try {
    const updatePaymentStatus = await prisma.member.update({
      where: { id: memberId },
      data: {
        isPaying: paymentStatus,
      },
    });

    return updatePaymentStatus;
  } catch (error) {
    console.error("Error updating payment status", error);
    return { error: "Failed to update payment status" };
  }
};