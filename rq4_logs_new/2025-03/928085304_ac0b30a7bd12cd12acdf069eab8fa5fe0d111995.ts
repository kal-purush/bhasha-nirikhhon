// app/api/orders/[orderId]/route.ts
import { NextResponse } from "next/server";
import { orders } from "@/lib/db";

export async function DELETE(
  request: Request,
  { params }: { params: { orderId: string } }
) {
  const orderId = parseInt(params.orderId, 10);
  const order = orders[orderId];
  if (!order) {
    return NextResponse.json({ error: "Order not found" }, { status: 404 });
  }
  try {
    const refundData = await request.json(); // Array<{ menuId, quantity }>
    refundData.forEach((refund: { menuId: number; quantity: number }) => {
      const index = order.items.findIndex((item) => item.menuId === refund.menuId);
      if (index !== -1) {
        if (order.items[index].quantity <= refund.quantity) {
          order.items.splice(index, 1);
        } else {
          order.items[index].quantity -= refund.quantity;
        }
      }
    });
    const message =
      order.items.length === 0 ? "취소가 완료 되었습니다." : "부분 취소가 완료되었습니다.";
    return NextResponse.json({ message });
  } catch (error) {
    return NextResponse.error();
  }
}