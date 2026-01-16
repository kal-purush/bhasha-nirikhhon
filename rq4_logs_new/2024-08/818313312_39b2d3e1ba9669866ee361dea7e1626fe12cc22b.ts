"use server";

import prisma from "@/lib/prisma-client";
import { Customer, ServiceDeliveryOffice, User, Vehicle } from "@prisma/client";

type ServiceResponse = { status: "success" | "error"; message: string };

export const saleVehicle = async (data: {
  saler: Customer[];
  buyer: Customer[];
  vehicle: Vehicle[];
  witness: Customer[];
  serviceDeliveringOffice: ServiceDeliveryOffice;
  user: User;
}): Promise<ServiceResponse> => {
  const { saler, buyer, vehicle, witness, serviceDeliveringOffice, user } =
    data;

  try {
    await prisma.service.create({
      data: {
        amount: 100,
        fileLocation: "/file-location",
        fileNumber: 2,
        numberOfPages: 3,
        filePath: "/file-path",
        printStatus: false,
        serviceDeliveryDate: new Date(),
        serviceRequestDate: new Date(),
        serviceSubType: "Vehicle",
        serviceType: "Sale",
        accountUser: {
          connectOrCreate: {
            where: { id: user.id },
            create: user,
          },
        },

        customers: {
          create: [...saler, ...buyer, ...witness],
        },
        vehicles: { create: vehicle },
        serviceDeliveryOffice: {
          connectOrCreate: {
            where: { id: serviceDeliveringOffice.id },
            create: serviceDeliveringOffice,
          },
        },
      },
    });
  } catch (error) {
    console.error("Error creating vehicle sale service:", error);
    return {
      status: "error",
      message: "An error occurred while creating the vehicle sale service",
    };
  }

  return {
    status: "success",
    message: "Vehicle sale service created successfully",
  };
};