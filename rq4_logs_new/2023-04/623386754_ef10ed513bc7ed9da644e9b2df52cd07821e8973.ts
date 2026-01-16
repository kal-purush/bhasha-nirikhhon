// MODELS
import { Product } from "../models";
// EXPRESS
import { Request, Response } from "express";
// INTERFACES
import { GetAllProductsQueryInterface } from "../utilities/interfaces";
// HTTP CODES
import { StatusCodes } from "http-status-codes";

const getAllProducts = async (req: Request, res: Response) => {
  // QUERY FROM THE CLIENT
  const {
    name,
    brand,
    color,
    size,
    price,
    isReview,
    isStock,
    rating,
    gender,
    page,
  } = req.body;
  // EMPTY QUERY IN SERVER TO SET VALUES
  const query: Partial<GetAllProductsQueryInterface> = {};
  if (name) query.name = name;
  if (brand) query.brand = brand;
  if (color) query.color = color;
  if (size) query.size = size;

  if (price) {
    // EXAMPLE: gte-50_lte-100
    let splitPrice: [string, string] = price.split("_");
    let gteVal: number;
    const priceVal: { $gte: number | undefined; $lte: number | undefined } = {
      $gte: undefined,
      $lte: undefined,
    };
    if (splitPrice[0] && splitPrice[0].startsWith("gte-")) {
      // EXAMPLE: [gte,50]
      const gte: string[] = splitPrice[0].split("-");
      // EXAMPLE: 50
      let gteVal: number = Number(gte[1]);
      // {$gte: 50}
      priceVal.$gte = gteVal;
    }
    if (splitPrice[0] && splitPrice[1].startsWith("lte-")) {
      // EXAMPLE: [lte,100]
      const lte: string[] = splitPrice[1].split("-");
      // EXAMPLE: 100
      let lteVal: number = Number(lte[1]);
      // {$lte: 100}
      priceVal.$lte = lteVal;
    }
  }

  if (isReview) query.isReview = isReview === "true";
  if (isStock) query.isStock = isStock === "true";
  if (rating) query.rating = Number(rating);
  if (gender) query.gender = gender;
  if (page) query.page = Number(page);
  else query.page = 1;

  const limit = 10;
  const skip = limit * (page - 1);
  const findProducts = Product.find({});

  const products = await findProducts.skip(skip).limit(limit);

  res.status(StatusCodes.OK).json({ products });
};