import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Order from "../../schema/order";

export const newOrder = async (req: Request, res: Response): Promise<void> => {
  try {
    const data: Order = req.body;

    const docRef = await db.collection("orders").add(data);
    res.status(201).json({ id: docRef.id });
  } catch (error) {
    console.error("Error al crear la orden", error);
    res.status(400).json({ messege: error.message });
  }
};

export const updateOrder = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
  } catch (error) {}
};