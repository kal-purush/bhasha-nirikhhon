"use server";
// add or remove car from favorites
import { prisma } from "@/db/prisma";
export async function addFavorite(userId: number, carId: number) {
  try {
    const favorite = await prisma.favorite.create({
      data: {
        userId: userId,
        carId: carId,
      },
    });
    return { success: true, message: "Car successfully added to favorites" };
  } catch (error: any) {
    console.error(error);
    return { success: false, error: error.message || " Something went wrong" };
  } finally {
    await prisma.$disconnect();
  }
}

export async function removeFavorite(userId: number, carId: number) {
  try {
    await prisma.favorite.deleteMany({
      where: {
        userId: userId,
        carId: carId,
      },
    });
    return {
      success: true,
      message: "Car successfully removed from favorites",
    };
  } catch (error: any) {
    console.error(error);
    return {
      success: false,
      error: error.message || " Something went wrong",
    };
  } finally {
    await prisma.$disconnect();
  }
}