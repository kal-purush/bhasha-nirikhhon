import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Order from "../../schema/order";
import { newOrder } from "../inputs/order";

export const searchOrder = async (
  req: Request,
  res: Response
): Promise<void> => {
  try {
    const id: string = req.params.id;
    const doc = await db.collection("orders").doc(id).get();
    if (!doc.exists) {
      res.status(404).json({ message: "Orden no encontrado" });
    } else {
      const orden = doc.data() as Order;
      res.status(201).json(orden);
    }
  } catch (error) {
    console.error("Error al obtener la orden", error);
    res.status(500).json({ message: "Error al obtener la orden" });
  }
};