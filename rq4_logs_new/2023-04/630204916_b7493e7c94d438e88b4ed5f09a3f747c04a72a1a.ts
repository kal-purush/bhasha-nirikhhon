import { Request, Response, NextFunction } from "express";
import { OwnerToRegister, OwnerToUpdate } from "../../schema/owner";

export const newOwnerValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: OwnerToRegister = req.body;
    if (!data.name || !data.email || !data.phone || !data.DNI || !data.vehiclesId)
      throw Error("Datos incompletos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

export const updateOwnerValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: OwnerToUpdate = req.body;
    const allowProperties = ["deleted", "name", "email", "phone", "adress", "DNI", "vehicleId"];
    if (Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Datos faltantes");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

export const deleteOwnerValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: OwnerToUpdate = req.body;
    const allowProperties = ["deleted"];
    if (Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Dato faltante");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};