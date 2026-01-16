import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Vehicle from "../../schema/vehicle";

export const newVehicle = async (req: Request, res: Response): Promise<void> => {
  try {
    const data: Vehicle = req.body;
    const snapshot = await db.collection("vehicle").where("patent", "==", data.patent).get();
    if (!snapshot.empty) {
      throw new Error("La patente del vehículo ya esta registrada");
    }
    const docRef = await db.collection("vehicle").add(data);
    res.status(201).json({ id: docRef.id });
  } catch (innerError) {
    console.error("Error al crear el vehículo", innerError);
    res.status(400).json({ message: innerError.message });
  }
};

export const updateVehicle = async (req: Request, res: Response): Promise<void> => {
  try {
    const id: string = req.params.id;
    const data: Vehicle = req.body;
    const docRef = await db.collection("vehicle").doc(id).get();
    if (!docRef.exists) {
      throw new Error("El vehículo no se actualizo");
    }
    await db.collection("vehicle").doc(id).update(data);
    res.status(200).json({ message: "Vehículo actualizado correctamente" });
  } catch (innerError) {
    console.error("Error al actualizar el vehículo", innerError);
    res.status(400).json({ message: innerError.message });
  }
};