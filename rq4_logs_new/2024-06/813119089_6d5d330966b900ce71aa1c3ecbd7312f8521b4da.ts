import { NextRequest, NextResponse } from "next/server";
import prisma from "../../../../lib/prisma";

export async function POST(req: Request) {
  const dataToInsert = await req.json();
  console.log(dataToInsert);
  try {
    const dataAvailable = await prisma.tb_recepcion.findFirst({
      where: {
        nropallet: dataToInsert.nropallet_recepcion,
      },
      select: {
        box_disponible: true,
        kg_disponible: true,
        codigo: true,
      },
    });

    console.log(dataAvailable);

    if (dataAvailable) {
      const boxDisponibleFromDb = dataAvailable.box_disponible;
      const kgDisponible = dataAvailable.kg_disponible.toNumber();

      if (kgDisponible !== 0 && boxDisponibleFromDb !== 0) {
        try {
          const updateTbReception = await prisma.tb_recepcion.update({
            where: {
              nropallet: dataToInsert.nropallet_recepcion,
              codigo: dataAvailable.codigo,
            },
            data: {
              box_disponible:
                boxDisponibleFromDb - Number(dataToInsert.numberBaxes),
              kg_disponible: kgDisponible - Number(dataToInsert.kgUsedBaxes),
              state: dataToInsert.state,
            },
          });

          console.log(updateTbReception);
        } catch (error) {
          console.log(error);
        }
      }
    }

    return NextResponse.json({ message: "Success", status: 200 });
  } catch (error) {
    console.log(error);
    return NextResponse.json(
      {
        message: "Error updating data",
        error: error,
      },
      { status: 500 }
    );
  }
}