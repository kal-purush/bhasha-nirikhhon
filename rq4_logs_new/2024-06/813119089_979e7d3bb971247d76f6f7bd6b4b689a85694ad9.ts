import { NextRequest, NextResponse } from "next/server";
import prisma from "../../../../../lib/prisma";

export async function GET(
  req: Request,
  { params }: { params: { id: number } }
) {
  const id = params.id;
  try {
    console.log("intru");
    const res = await prisma.tb_delivery_pallet.update({
      where: {
        iddelivery: Number(id),
      },
      data: {
        state: 2,
      },
    });

    return NextResponse.json({ message: "Succes", response: res, status: 200 });
  } catch (error) {
    console.log(error);
    return {
      message: "Error fetching data",
      error: error
    }
  }
}