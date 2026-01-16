"use server";

import { Drone, PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export async function AssignFarmer({
  farmer,
  drone,
}: {
  farmer: string;
  drone: string | null;
}) {
  try {
    if (farmer && drone) {
      const farmerData = await prisma.farmer.findUnique({
        where: { id: farmer },
      });
      const droneData = await prisma.drone.findUnique({
        where: { id: parseInt(drone) },
      });

      if (farmerData && droneData) {
        if (farmerData.droneId === null) {
          await prisma.farmer.update({
            where: { id: farmer },
            data: {
              droneId: droneData.id,
            },
          });

          await prisma.drone.update({
            where: { id: droneData.id },
            data: {
              availability: "BUSY",
            },
          });
        }
      }
    }
  } catch (error) {
    console.log(error);
  } finally {
    await prisma.$disconnect();
  }
}