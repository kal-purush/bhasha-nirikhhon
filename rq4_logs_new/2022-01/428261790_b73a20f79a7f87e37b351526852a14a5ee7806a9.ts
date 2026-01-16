import { Prisma, Product } from "@prisma/client";
import prisma from "../lib/prisma";

export interface CartProduct {
  id: number;
  name: string;
  desc: string;
  price: Prisma.Decimal;
  amount: number;
  categoryId: number;
  categoryName: String;
}

export async function getCart(cartId: number): Promise<CartProduct[]> {
  const cart = await prisma.order.findFirst({
    where: { id: cartId },
    include: {
      OrderProduct: {
        include: {
          Product: {
            include: { category: true },
          },
        },
      },
    },
  });

  let myCart: CartProduct[] = [];
  cart.OrderProduct.forEach((op) => {
    const p = op.Product;
    myCart.push({
      id: p.id,
      name: p.name,
      desc: p.desc,
      price: p.price,
      amount: op.amount,
      categoryId: p.categoryId,
      categoryName: p.category.name,
    });
  });
  return myCart;
}