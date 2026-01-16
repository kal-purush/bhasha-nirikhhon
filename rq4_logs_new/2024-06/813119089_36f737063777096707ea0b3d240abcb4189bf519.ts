import { NextResponse } from "next/server";
import prisma from "../../../../../lib/prisma";

export async function GET(
  req: Request,
  { params }: { params: { idorden_idpunnet: string } }
) {
  const [idorden, idpunnet] = params.idorden_idpunnet.split("_");

  try {
    const data = await prisma.tb_delivery_pallet.findMany({
      where: {
        idorden: Number(idorden),
        idpunnet_orden: Number(idpunnet),
      },
    });

    console.log(data);

    return NextResponse.json(data);
  } catch (error) {
    console.error("Error fetching data:", error);
    return NextResponse.json(
      {
        message: "Error fetching data",
        error: error,
      },
      { status: 500 }
    );
  }
}