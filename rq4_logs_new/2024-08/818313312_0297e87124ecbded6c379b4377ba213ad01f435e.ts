"use server";

import { toCustomerDb, toVehicleDb } from "@/_utils/dto";
import prisma from "@/lib/prisma-client";
import { ServiceDeliveryOffice, User } from "@prisma/client";

type ServiceResponse = { status: "success" | "error"; message: string };

export const getServiceVehicle = async (): Promise<null | any[]> => {
  console.log("I am called too");
  let data : any
  try {
    data = await prisma.vehicle.findMany({
        include : {
          service : {}
        }
    });
  } catch (error) {
    console.error("Error creating vehicle sale service:", error);
    return null;
  }
  console.log(data)

  return data;
};


export const deleteServiceVehicle = async (id : string): Promise<null | any[]> => {
  console.log("Deleting");
  let data : any
  try {
    data = await prisma.vehicle.delete({
     where : {
      id : id
     }
    });
  } catch (error) {
    console.error("Error deleting vehicle sale service:", error);
    return null;
  }
  console.log(data)

  return data;
};