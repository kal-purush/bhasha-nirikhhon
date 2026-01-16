import prisma from "../../lib/prisma";

export const updateStateDeliveryPalett = async (iddelivery: number) => {
  console.log(iddelivery);
  try {
    console.log("intru");
    await prisma.tb_delivery_pallet.update({
      where: {
        iddelivery: iddelivery,
      },
      data: {
        state: 2,
      },
    });

    return {
      message: "Successfully updated",
    };
  } catch (error) {
    console.log(error);
    return {
      message: "Error fethcing date",
      error: error,
    };
  }
};