import { Request, Response, NextFunction } from "express";
import { Products, ProductsToUpdate } from "../../schema/products";

export const newProductValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: Products = req.body;
    if (!data.name || !data.price || !data.description || !data.img || !data.type || !data.stock || data.localId )
      throw Error("Datos incompletos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

export const updateProductValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: ProductsToUpdate = req.body;
    const allowProperties = ["name", "price", "description", "img", "type", "stock", "deleted"];
    if (Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Datos no permitidos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};