import { NextRequest, NextResponse } from "next/server";
import prisma from "../../../../lib/prisma";

export async function GET() {
  try {
    const data = await prisma.tb_orden.findMany({
      where: {
        state: 6,
      },
      select: {
        cliente: true,
        destino: true,
        fecha_despacho: true,
        idorden: true,
      },
    });

    return NextResponse.json(data); // Return the fetched data as JSON response
  } catch (error) {
    console.error("Error fetching data:", error);
    return NextResponse.json(
      { message: "Internal Server Error" },
      { status: 500 }
    );
  }
}