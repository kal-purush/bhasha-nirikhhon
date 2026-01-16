import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Owner from "../../schema/owner";

export const newOwner = async (req: Request, res: Response): Promise<void> => {
  try {
    const data: Owner = req.body;
    const snapshot = await db.collection("owner").where("DNI", "==", data.DNI).get();
    if (!snapshot.empty) {
      throw new Error("El DNI ya está registrado");
    }
    const docRef = await db.collection("owner").add(data);
    res.status(201).json({ id: docRef.id });
  } catch (innerError) {
    console.error("Error al crear el propietario", innerError);
    res.status(400).json({ message: innerError.message });
  }
};

export const updateOwner = async (req: Request, res: Response): Promise<void> => {
  try {
    const id: string = req.params.id;
    const data: Owner = req.body;
    const docRef = await db.collection("owner").doc(id).get();
    if (!docRef.exists) {
      throw new Error("No se encontró el propietario");
    }
    await db.collection("owner").doc(id).update(data);
    res.status(200).json({ message: "Propietario actualizado correctamente" });
  } catch (innerError) {
    console.error("Error al actualizar el propietario", innerError);
    res.status(400).json({ message: innerError.message });
  }
};