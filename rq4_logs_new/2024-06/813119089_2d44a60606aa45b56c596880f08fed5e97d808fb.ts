import { NextRequest, NextResponse } from "next/server";
import prisma from "../../../../../lib/prisma";

export async function GET(
  req: Request,
  { params }: { params: { product: any } }
) {
  if (req.method !== "GET") {
    return NextResponse.json(
      { message: "Method not allowed" },
      { status: 405 }
    );
  }
  console.log("intru");
  const product = params.product;

  try {
    const orderDetail = await prisma.tb_product.findFirst({
      where: {
        product: product, // Convert idorden to a number if needed
      },
      select: {
        weight: true,
      },
    });

    return NextResponse.json({
      message: "Your order detail",
      orderDetail: orderDetail, // Corrected key name
    });
  } catch (error) {
    console.error(error);
    return NextResponse.json(
      { message: "Internal Server Error" },
      { status: 500 }
    );
  }
}