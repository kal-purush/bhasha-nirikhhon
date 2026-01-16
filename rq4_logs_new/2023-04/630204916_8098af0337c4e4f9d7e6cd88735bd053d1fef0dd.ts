import { Request, Response } from "express";
import { db } from "../../connection/connection";
import { LocalToRegister } from "../../interfaces/local";

/**
 * Controlador para crear un local en Firestore.
 */
export const newLocal = async (req: Request, res: Response): Promise<void> => {
  try {
    const data: LocalToRegister = req.body;

    const docRef = await db.collection("locals").add(data);
    res.status(201).json({ id: docRef.id });
  } catch (error) {
    try {
      throw new Error(error.message);
    } catch (innerError) {
      console.error("Error al crear el local", innerError);
      res.status(400).json({ message: innerError.message });
    }
  }
};