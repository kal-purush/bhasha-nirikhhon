import prisma from "../../../lib/prisma";
import { NextApiRequest, NextApiResponse } from "next";

export default async (req: NextApiRequest, res: NextApiResponse) => {
  const productId = req.query["productId"] as string;
  let pid = parseInt(productId);
  if (isNaN(pid)) {
    return res.status(400).json({ message: "Id must be number" });
  }

  const productObj = await prisma.product.findFirst({
    where: {
      id: pid,
      published: true,
      stockAmount: {
        not: 0,
      },
    },
    select: {
      id: true,
      name: true,
      desc: true,
      imageLink: true,
      price: true,
    },
  });
  if (productObj == undefined) {
    res.status(404).json({ error: "not found" });
    return;
  }
  res.status(200).json(productObj);
};