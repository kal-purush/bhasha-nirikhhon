import { Request, Response, NextFunction } from "express";
import { ChauffeurToRegister, ChauffeurToUpdate } from "../../schema/chauffeur";

export const newChauffeurValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: ChauffeurToRegister = req.body;
    if (
      !data.name ||
      !data.email ||
      !data.password ||
      !data.phone ||
      !data.DNI ||
      !data.license ||
      !data.address
    )
      throw Error("Datos incompletos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

export const updateChauffeurValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: ChauffeurToUpdate = req.body;
    const allowProperties = [
      "deleted",
      "email",
      "password",
      "phone",
      "adress",
      "vehicle.vehicleId",
      "vehicle.patent",
      "payments.cardNumber",
      "payments.expirationDate",
      "payments.securityCode",
    ];
    if (Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Datos no permitidos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};