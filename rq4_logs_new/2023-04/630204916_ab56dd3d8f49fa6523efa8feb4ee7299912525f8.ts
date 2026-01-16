import { Request, Response, NextFunction } from "express";
import { LocalToRegister, LocalToUpdate } from "../../schema/local";

export const newLocalValidate = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: LocalToRegister = req.body;
    if (!data.name || !data.adress || !data.email || !data.password || !data.img)
      throw Error("Datos incompletos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

// FALTA TESTEAR DESPUES DE REINICIAR LA BD Y CREAR TODOS LOS MODELOS CON EL CAMPO DE BORRADO LÓGICO
export const updateLocalValidate = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: LocalToUpdate = req.body;
    const allowProperties = ["name", "adress", "email", "password", "img"];
    if (!Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Datos no permitidos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};