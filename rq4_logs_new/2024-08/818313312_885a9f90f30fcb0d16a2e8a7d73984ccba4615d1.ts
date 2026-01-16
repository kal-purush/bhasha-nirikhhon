"use server";

import {
  toCustomerDb,
  toLeaseDb,
  toMotorcycleDb,
  toPropertyDb,
  toResidenceDb,
  toVehicleDb,
} from "@/_utils/dto";
import prisma from "@/lib/prisma-client";
import { ServiceDeliveryOffice, User } from "@prisma/client";

type ServiceResponse = { status: "success" | "error"; message: string };

const DefaultServiceProperties = {
  amount: 100,
  fileLocation: "/file-location",
  fileNumber: 2,
  numberOfPages: 3,
  filePath: "/file-path",
  printStatus: false,
  serviceDeliveryDate: new Date(),
  serviceRequestDate: new Date(),
};

type ServiceCommonProps = {
  customers: ReturnType<typeof toCustomerDb>[];
  serviceDeliveringOffice: Omit<ServiceDeliveryOffice, "id"> & { id?: string };
  user: Omit<User, "id"> & { id?: string };
  serviceType: string;
};

export const vehicleService = async (
  data: ServiceCommonProps & {
    vehicles: ReturnType<typeof toVehicleDb>[];
  },
): Promise<ServiceResponse> => {
  const { customers, vehicles, serviceDeliveringOffice, user, serviceType } =
    data;

  try {
    await prisma.service.create({
      data: {
        ...DefaultServiceProperties,
        serviceSubType: "Vehicle",
        serviceType,
        accountUser: {
          connectOrCreate: {
            where: { id: user.id || "" },
            create: user,
          },
        },

        customers: {
          create: customers,
        },
        vehicles: { create: vehicles },
        serviceDeliveryOffice: {
          connectOrCreate: {
            where: { id: serviceDeliveringOffice.id || "" },
            create: serviceDeliveringOffice,
          },
        },
      },
    });
  } catch (error) {
    return {
      status: "error",
      message: "An error occurred while creating the vehicle service",
    };
  }

  return {
    status: "success",
    message: "Vehicle service created successfully",
  };
};

export const motorcycleService = async (
  data: ServiceCommonProps & {
    motorcycles: ReturnType<typeof toMotorcycleDb>[];
  },
): Promise<ServiceResponse> => {
  const { customers, motorcycles, serviceDeliveringOffice, user, serviceType } =
    data;

  try {
    await prisma.service.create({
      data: {
        ...DefaultServiceProperties,
        serviceType,
        serviceSubType: "Motorcycle",
        accountUser: {
          connectOrCreate: {
            where: { id: user.id || "" },
            create: user,
          },
        },

        customers: {
          create: customers,
        },
        motorcycles: { create: motorcycles },
        serviceDeliveryOffice: {
          connectOrCreate: {
            where: { id: serviceDeliveringOffice.id || "" },
            create: serviceDeliveringOffice,
          },
        },
      },
    });
  } catch (error) {
    return {
      status: "error",
      message: "An error occurred while creating the Motorcycles service",
    };
  }

  return {
    status: "success",
    message: "Motorcycles service created successfully",
  };
};
export const residenceService = async (
  data: ServiceCommonProps & {
    residences: ReturnType<typeof toResidenceDb>[];
  },
): Promise<ServiceResponse> => {
  const { customers, residences, serviceDeliveringOffice, user, serviceType } =
    data;

  try {
    await prisma.service.create({
      data: {
        ...DefaultServiceProperties,
        serviceType,
        serviceSubType: "Residence",
        accountUser: {
          connectOrCreate: {
            where: { id: user.id || "" },
            create: user,
          },
        },

        customers: {
          create: customers,
        },
        residences: { create: residences },
        serviceDeliveryOffice: {
          connectOrCreate: {
            where: { id: serviceDeliveringOffice.id || "" },
            create: serviceDeliveringOffice,
          },
        },
      },
    });
  } catch (error) {
    return {
      status: "error",
      message: "An error occurred while creating the Residence service",
    };
  }

  return {
    status: "success",
    message: "Residence service created successfully",
  };
};
export const leaseService = async (
  data: ServiceCommonProps & {
    leases: ReturnType<typeof toLeaseDb>[];
  },
): Promise<ServiceResponse> => {
  const { customers, leases, serviceDeliveringOffice, user, serviceType } =
    data;

  try {
    await prisma.service.create({
      data: {
        ...DefaultServiceProperties,
        serviceType,
        serviceSubType: "Lease",
        accountUser: {
          connectOrCreate: {
            where: { id: user.id || "" },
            create: user,
          },
        },

        customers: {
          create: customers,
        },
        leases: { create: leases },
        serviceDeliveryOffice: {
          connectOrCreate: {
            where: { id: serviceDeliveringOffice.id || "" },
            create: serviceDeliveringOffice,
          },
        },
      },
    });
  } catch (error) {
    return {
      status: "error",
      message: "An error occurred while creating the Lease service",
    };
  }

  return {
    status: "success",
    message: "Lease service created successfully",
  };
};
export const propertyService = async (
  data: ServiceCommonProps & {
    properties: ReturnType<typeof toPropertyDb>[];
  },
): Promise<ServiceResponse> => {
  const { customers, properties, serviceDeliveringOffice, user, serviceType } =
    data;

  try {
    await prisma.service.create({
      data: {
        ...DefaultServiceProperties,
        serviceType,
        serviceSubType: "Propery",
        accountUser: {
          connectOrCreate: {
            where: { id: user.id || "" },
            create: user,
          },
        },

        customers: {
          create: customers,
        },
        properties: { create: properties },
        serviceDeliveryOffice: {
          connectOrCreate: {
            where: { id: serviceDeliveringOffice.id || "" },
            create: serviceDeliveringOffice,
          },
        },
      },
    });
  } catch (error) {
    return {
      status: "error",
      message: "An error occurred while creating the Property service",
    };
  }

  return {
    status: "success",
    message: " service created successfully",
  };
};

// Todo:
export const vehicleSparepartsService = async (
  data: ServiceCommonProps & {
    vehicleSpareparts: any;
  },
): Promise<any> => {};