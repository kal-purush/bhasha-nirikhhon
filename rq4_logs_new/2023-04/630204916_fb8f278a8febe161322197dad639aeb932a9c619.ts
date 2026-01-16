import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Distributor from "../../schema/distributor";

export const newDistributor = async (req: Request, res: Response) => {
  try {
    const data: Distributor = req.body;

    // Verificar si ya existe un distribuidor con el correo electrónico dado
    const snapshot = await db
      .collection("distributors")
      .where("email", "==", data.email)
      .get();
    if (!snapshot.empty) {
      throw new Error("El correo electrónico ya está registrado");
    }
    //crear doocumento de distribuidor
    const docRef = await db.collection("distributors").add(data);
    res.status(201).json({ id: docRef.id });
  } catch (error) {
    console.error("Error al actualizar el usuario", error);
    res.status(400).json({ messege: error.message });
  }
};