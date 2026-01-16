"use server";
import { prisma } from "@/db/prisma";

export type Car = {
  id: number;
  modelName: string;
  image: string;
  year: number;
  pricePerDay: number;
  transmissionType: string;
  bodyType: string;
  fuelConsumption: number;
  seats: number;
  fuelType: string;
  isRented: boolean;
  rating?: number;
  location: string;
  createdAt?: string;
  reviews?: [];
  bookings?: [];
  favorites?: [];
};

export default async function fetchCars() {
  try {
    const cars = await prisma.car.findMany();
    return cars;
  } catch (error) {
    console.error("Error fetching cars from database:", error);
    // Fallback to external API
    try {
      const response = await fetch(
        "https://basalt-equatorial-paw.glitch.me/cars"
      );
      const carsData = await response.json();
      return carsData as Car[];
    } catch (fallbackError) {
      console.error("Error fetching cars from fallback API:", fallbackError);
      return null;
    }
  } finally {
    await prisma.$disconnect();
  }
}

export async function fetchCar(id: string) {
  try {
    const car = await prisma.car.findUnique({
      where: {
        id: Number(id),
      },
    });
    return car;
  } catch (error) {
    console.error("Error fetching car:", error);
    return null;
  } finally {
    await prisma.$disconnect();
  }
}