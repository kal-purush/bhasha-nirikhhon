"use server";

import { prisma } from "@/lib/prisma";
import { getNumberArray } from "@/lib/utils";
import { AddPaymentFormSchema } from "@/schemas";
import * as z from "zod";

interface payRugbyPaymentProps {
  memberId: string;
  values: z.infer<typeof AddPaymentFormSchema>;
}

export const updatePaymentRecord = async ({
  memberId,
  values,
}: payRugbyPaymentProps) => {
  const validateFields = AddPaymentFormSchema.safeParse(values);

  if (!validateFields.success) {
    return { error: "Invalid fields" };
  }

  const finalValues = validateFields.data;

  try {
    const updatedRugbyPayment = await prisma.member.update({
      where: { id: memberId },
      data: {
        paymentRecord: getNumberArray(finalValues),
      },
    });

    return updatedRugbyPayment;
  } catch (error) {
    console.error("Error updating rugby payment:", error);
    return { error: "Failed to update rugby payment" };
  }
};