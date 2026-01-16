import { Vehicle, VehicleToUpdate } from "../../schema/vehicle";
import { Request, Response, NextFunction } from "express";

export const newVehicleValidate = (req: Request, res: Response, next: NextFunction): void => {
  // Validar que todas las propiedades tengan un valor válido
  try {
    const data: Vehicle = req.body;
    const allowProperties = [
      "patent",
      "brand",
      "model",
      "year",
      "img",
      "ownerId",
      "chauffeurId",
      "deleted",
    ];
    if (Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Datos no permitidos");
    if (
      !data.patent ||
      !data.brand ||
      !data.model ||
      !data.year ||
      !data.img ||
      !data.ownerId ||
      !data.chauffeurId ||
      !data.deleted
    )
      throw new Error("Faltan datos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

// export const updateDistributorValidate = (
//   req: Request,
//   res: Response,
//   next: NextFunction
// ): void => {
//   try {
//     const data: DistributorToUpdate = req.body;
//     const allowProperties: string[] = [
//       "name",
//       "adress",
//       "email",
//       "password",
//       "img",
//       "vehicle",
//       "dni",
//       "license",
//     ];
//     if (Object.keys(data).some((key) => !allowProperties.includes(key)))
//       throw new Error("Datos no permitidos");
//     next();
//   } catch (error) {
//     res.status(400).json({ message: error.message });
//   }
// };

// export const deleteDistributorValidate = (
//   req: Request,
//   res: Response,
//   next: NextFunction
// ): void => {
//   try {
//     const data: DistributorToUpdate = req.body;
//     const allowProperties: string[] = ["delete"];
//     if (Object.keys(data).some((key) => !allowProperties.includes(key)))
//       throw new Error("Datos no permitidos");
//     next();
//   } catch (error) {
//     res.status(400).json({ message: error.message });
//   }
// };