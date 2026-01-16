import { NextApiRequest, NextApiResponse } from "next";
import { addProductToCart } from "../../../lib/shopApi";
import assert from "assert";

function getCartId(req: NextApiRequest) {
  const cartIdRaw = req.cookies["cart_id"];
  if (cartIdRaw == undefined || isNaN(parseInt(cartIdRaw))) {
    return -1;
  }
  return parseInt(cartIdRaw);
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
    assert.notEqual(null, req.body.productId, "productId required");
    assert.equal("number", typeof req.body.productId, "productId must be int");
    assert.notEqual(null, req.body.amount, "amount required");
    assert.equal("number", typeof req.body.amount, "amount must be int");
  } catch (bodyError) {
    res.status(403).json({ message: bodyError.message });
    return;
  }

  // do
  await addProductToCart(cartId, req.body.productId, req.body.amount);

  res.status(200).json({ message: "Product added to cart" });
  return;
};