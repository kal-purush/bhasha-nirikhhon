"use server";
import { prisma } from "@/db/prisma";

export interface DiscountData {
  code: string;
  percent: number;
  description: string;
  expiresAt: string;
  carId?: number;
  min_amount?: number;
  max_amount?: number;
}
export async function createNewCoupon(formData: DiscountData) {
  try {
    const coupon = await prisma.discount.create({
      data: { ...formData },
    });
    return { success: true, message: "Coupon created successfully", coupon };
  } catch (error) {
    console.error(error);
    return { success: false, error: "Something went wrong" };
  } finally {
    await prisma.$disconnect();
  }
}