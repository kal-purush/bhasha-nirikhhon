import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Distributor from "../../schema/distributor";

export const searchDistributor = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    console.log(id);
    const doc = await db.collection("distributors").doc(id).get();
    if (!doc.exists) {
      res.status(404).json({ message: "Distribuidor no encontrado" });
    } else {
      const distribuidor = doc.data() as Distributor;
      res.status(201).json(distribuidor);
    }
  } catch (error) {
    console.error("Error al obtener el usuario", error);
    res.status(500).json({ message: "Error al obtener el distribuidor" });
  }
};

export const allDistributors = async (req: Request, res: Response) => {
  try {
    res.status(201).json({ status: "todo ok" });
  } catch (error) {
    console.error("Error al obtener el usuario", error);
    res.status(500).json({ message: "Error al obtener los distribuidores" });
  }
};