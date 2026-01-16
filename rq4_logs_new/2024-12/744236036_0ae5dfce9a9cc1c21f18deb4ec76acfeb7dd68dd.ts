"use server";
import { prisma } from "@/db/prisma";

export async function createNewCar(Data: any) {
  try {
    await prisma.car.create({
      data: { ...Data },
    });
    return { success: true, message: "Car created successfully" };
  } catch (error: any) {
    if (
      error.code === "P2002" // Unique constraint violation
    ) {
      return { success: false, error: "Car already exists" };
    }
    return { success: false, error: "Something went wrong" };
  } finally {
    await prisma.$disconnect();
  }
}