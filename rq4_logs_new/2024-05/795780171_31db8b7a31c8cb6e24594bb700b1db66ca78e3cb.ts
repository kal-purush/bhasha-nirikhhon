"use server";

import { prisma } from "@/lib/prisma";
import { AddPaymentFormSchema } from "@/schemas";
import * as z from "zod";

interface payRugbyPaymentProps {
  memberId: string;
  values: z.infer<typeof AddPaymentFormSchema>;
}

export const updatedRugbyPayment = async ({
  memberId,
  values,
}: payRugbyPaymentProps) => {
  const validateFields = AddPaymentFormSchema.safeParse(values);

  if (!validateFields.success) {
    return { error: "Invalid fields" };
  }

  const { jan, fev, mar, abr, mai, jun, jul, ago, set, out, nov, dez } =
    validateFields.data;

  try {
    const updatedRugbyPayment = await prisma.rugbyPayment.update({
      where: { memberId },
      data: {
        monthsPayment: [
          parseInt(jan),
          parseInt(fev),
          parseInt(mar),
          parseInt(abr),
          parseInt(mai),
          parseInt(jun),
          parseInt(jul),
          parseInt(ago),
          parseInt(set),
          parseInt(out),
          parseInt(nov),
          parseInt(dez),
        ],
      },
    });

    return updatedRugbyPayment;
  } catch (error) {
    console.error("Error updating rugby payment:", error);
    return { error: "Failed to update rugby payment" };
  }
};