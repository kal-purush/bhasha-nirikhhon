import { Request, Response } from "express";
import { db } from "../../connection/connection";
import Vehicle from "../../schema/vehicle";

export const searchAllVehicles = async (req: Request, res: Response): Promise<void> => {
  try {
    const vehicleRef = db.collection("vehicle");
    const vehicleSnapshot = await vehicleRef.get();
    const vehicles = [];
    vehicleSnapshot.forEach((doc) => {
      const vehicle = {
        id: doc.id,
        ...doc.data(),
      };
      vehicles.push(vehicle);
    });
    res.json(vehicles);
  } catch (innerError) {
    console.error("Error al encontrar los vehículos", innerError);
    res.status(400).json({ message: innerError.message });
  }
};

export const searchVehicleByPatent = async (req: Request, res: Response): Promise<void> => {
  try {
    const patent: string = req.params.patent;
    const doc = await db.collection("vehicle").where("patent", "==", patent).get();
    if (doc.empty) {
      res.status(404).json({ message: `No se encontró ningún vehículo con la patente ${patent}` });
    }
    const vehicleData = doc.docs[0].data() as Vehicle;
    res.json(vehicleData);
  } catch (innerError) {
    console.error("Error al encontrar el vehículo por patente", innerError);
    res.status(400).json({ message: innerError.message });
  }
};

export const searchVehicleByOwner = async (req: Request, res: Response): Promise<void> => {
  try {
    const owner: string = req.params.owner;
    const doc = await db.collection("vehicle").where("owner", "==", owner).get();
    if (doc.empty) {
      res.status(404).json({ message: `No se encontró ningún vehículo en propiedad de ${owner}` });
    }
    const vehicleData = doc.docs[0].data() as Vehicle;
    res.json(vehicleData);
  } catch (innerError) {
    console.error("Error al encontrar el vehículo por propietario", innerError);
    res.status(400).json;
  }
};

export const searchVehicleByYear = async (req: Request, res: Response): Promise<void> => {
  try {
    const year: string = req.params.year;
    const vehicleRef = db.collection("vehicle");
    const vehicleSnapshot = await vehicleRef.where("year", "==", year).get();
    const vehicles = [];
    vehicleSnapshot.forEach((doc) => {
      const vehicle = {
        id: doc.id,
        ...doc.data(),
      };
      vehicles.push(vehicle);
    });
    res.json(vehicles);
  } catch (innerError) {
    console.error("Error al encontrar el vehículo por año", innerError);
    res.status(400).json;
  }
};