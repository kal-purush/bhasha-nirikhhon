"use server";
// All functions related to booking CRUD operations
import { prisma } from "@/db/prisma";

type Status = "ongoing" | "cancelled" | "completed";
export async function UpdateOrderStatus(id: number, status: Status) {
  try {
    await prisma.booking.update({
      where: { id },
      data: {
        status: status,
      },
    });
    return { success: true, message: "Booking status updated successfully!" };
  } catch (error) {
    console.error(error);
    return {
      success: false,
      error: error instanceof Error ? error.message : "Something went wrong",
    };
  }
}