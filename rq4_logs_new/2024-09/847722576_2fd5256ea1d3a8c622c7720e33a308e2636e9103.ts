import { NextResponse } from "next/server";
import {
  LineItemResponse,
  OrderResponse,
} from "@/app/interface/OrderInterface";

export async function POST(req: Request) {
  try {
    const accessToken = process.env.NEXT_SERVER_JWT_TEST as string;
    const { order } = await req.json();
    console.log(order);

    const response = await fetch("http://localhost:5000/api/calculate-order", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: accessToken,
      },
      body: JSON.stringify(order),
    });

    const { result } = await response.json();
    if (response.status !== 200) {
      throw new Error("Error calculating the order, check API");
    }

    const lineItemDetails = order.lineItems.map(
      (lineItem: LineItemResponse) => {
        return {
          uid: lineItem.catalogObjectId,
          totalTaxMoney: lineItem.totalTaxMoney.amount,
          totalDiscountMoney: lineItem.totalDiscountMoney.amount,
          totalMoney: lineItem.totalMoney.amount,
        };
      }
    );

    const orderResponse: OrderResponse = {
      totalTaxAmount: result.totalTaxAmount.amount,
      totalDiscountAmount: result.totalDiscountAmount.amount,
      totalAmount: result.totalAmount.amount,
    };

    return NextResponse.json(
      { data: { orderResponse, lineItemDetails } },
      { status: 200 }
    );
  } catch (error) {
    return NextResponse.json({ error }, { status: 500 });
  }
}