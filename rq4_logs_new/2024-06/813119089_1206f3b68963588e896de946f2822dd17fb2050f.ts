
import { NextRequest, NextResponse } from "next/server";
import prisma from "../../../../lib/prisma";

export async function POST(
  req: Request,
  { params }: { params: any }
) {
    console.log(params);
  try {
    const dataAvailable = await prisma.tb_recepcion.findFirst({where: {
        nropallet: params.nropallet_recepcion
    }, select: {
        box_disponible: true,
        kg_disponible: true,
    }});

    console.log("Data avail", dataAvailable);

    return NextResponse.json({ message: "Succes", status: 200 });
  } catch (error) {
    console.log(error);
    return NextResponse.json(
      {
        message: "Error fetching data",
        error: error,
      },
      { status: 500 }
    );
  }
}