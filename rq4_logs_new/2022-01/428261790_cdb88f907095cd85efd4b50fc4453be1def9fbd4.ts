import { NextApiRequest, NextApiResponse } from "next";
import prisma from "../../../lib/prisma";
import Cookies from "cookies";

async function cartIdCookieCheck(req: NextApiRequest) {
  const cartIdRaw = req.cookies["cart_id"];
  let cartId = 0;

  if (cartIdRaw == undefined || isNaN(parseInt(cartIdRaw))) {
    // cart dont exist, create new
    const lastCart = await prisma.order.findFirst({
      orderBy: {
        id: "desc",
      },
    });
    cartId = lastCart.id;
    console.log("Create new cart_id");
    return cartId; // nowe ciasteczko
  } else {
    cartId = parseInt(cartIdRaw);
  }

  return cartId;
}

async function checkCartExist(cartId: number) {
  const c = await prisma.order.findFirst({
    where: { id: cartId },
  });
  if (c == null) {
    // create new
    await prisma.order.create({
      data: {
        totalCost: 0.0,
      },
    });
  }
}

export default async (req: NextApiRequest, res: NextApiResponse) => {
  const cartIdCookie = await cartIdCookieCheck(req);
  await checkCartExist(cartIdCookie);

  if (cartIdCookie != null) {
    const cookies = new Cookies(req, res);
    cookies.set("cart_id", `${cartIdCookie}`, {
      httpOnly: true, // true by default
    });
  }

  res.status(200).json({ message: "ok" });
  return;
};