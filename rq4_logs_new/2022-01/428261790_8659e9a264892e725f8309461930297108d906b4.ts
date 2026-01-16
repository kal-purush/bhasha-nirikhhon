import { NextApiRequest, NextApiResponse } from "next";
import { addProductToCart } from "../../../lib/shopApi";
import assert from "assert";
import prisma from "../../../lib/prisma";
import Cookies from "cookies";

function getCartId(req: NextApiRequest) {
  const cartIdRaw = req.cookies["cart_id"];
  if (cartIdRaw == undefined || isNaN(parseInt(cartIdRaw))) {
    return -1;
  }
  return parseInt(cartIdRaw);
}

function eazyCheck(value: any, type: string, errorMsg: string) {
  assert.notEqual(null, value, errorMsg);
  assert.equal(type, typeof value, errorMsg);
}

export default async (req: NextApiRequest, res: NextApiResponse) => {
  if (req.method != "POST") {
    return res.status(400).json({ message: "POST method required" });
  }
  const cartId = getCartId(req);
  if (cartId == -1) {
    return res.status(400).json({ message: "missing cart_id cookie" });
  }

  // check data
  try {
    eazyCheck(req.body.firstName, "string", "firstName must be string");
    eazyCheck(req.body.lastName, "string", "lastName must be string");
    eazyCheck(req.body.street, "string", "street must be string");
    eazyCheck(req.body.building, "string", "building must be string");
    // eazyCheck(req.body.floor, "string", "floor must be string");
    eazyCheck(req.body.postalCode, "string", "postalCode must be string");
    eazyCheck(req.body.city, "string", "city must be string");
    eazyCheck(req.body.phone, "string", "phone must be string");
    eazyCheck(req.body.email, "string", "email must be string");
    // extraDetails
  } catch (bodyError) {
    res.status(403).json({ message: bodyError.message });
    return;
  }

  // do
  const thisOrder = await prisma.order.findFirst({
    where: { id: cartId },
    include: {
      OrderProduct: {
        include: {
          Product: true,
        },
      },
    },
  });

  if (thisOrder == null) {
    res.status(404).json({ message: "cart not found" });
    return;
  }

  const customer = await prisma.customer.create({
    data: {
      firstName: req.body.firstName,
      lastName: req.body.lastName,
      street: req.body.street,
      // req.body.building
      // req.floor
      city: req.body.city,
      postalCode: req.body.postalCode,
      // phone
      email: req.body.email,
    },
  });

  // total value
  let totalCost = 0;
  totalCost += 15.99; // kurier
  thisOrder.OrderProduct.forEach((op) => {
    totalCost += parseInt(op.Product.price as unknown as string) * op.amount;
  });

  await prisma.order.update({
    where: { id: thisOrder.id },
    data: {
      customerId: customer.id,
      totalCost: totalCost,
    },
  });

  const cookies = new Cookies(req, res);
  const dtNow = new Date();
  cookies.set("cart_id", "0", { expires: dtNow, httpOnly: true });
  
  res.status(200).json({ message: "Zamówienie zostało złożone" });
  return;
};