import { Request, Response } from "express";
import { db } from "../../connection/connection";
import { Local } from "../../interfaces/local";

export const searchLocal = async (req: Request, res: Response): Promise<void> => {
  try {
    const { name } = req.params;
    const doc = await db.collection("locals").doc(name).get();
    if (!doc.exists) {
      res.status(404).json({ message: "Local no encontrado" });
    } else {
      const local = doc.data() as Local;
      res.json(local);
    }
  } catch (error) {
    console.error("Error al obtener el local", error);
    res.status(500).json({ message: "Error al obtener el local" });
  }
};