import { Request, Response, NextFunction } from "express";
import { UserToRegister, UserToUpdate } from "../../schema/user";

export const newUserValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: UserToRegister = req.body;
    if (!data.name || !data.email || !data.password)
      throw Error("Datos incompletos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

export const updateUserValidated = (req: Request, res: Response, next: NextFunction): void => {
  try {
    const data: UserToUpdate = req.body;
    const allowProperties = ["name", "adress", "email", "password", "img", "payments.cardNumber", "payments.cardExpiration", "payments.securityCode", "DNI", "deleted"];
    if (Object.keys(data).some((key) => !allowProperties.includes(key)))
      throw Error("Datos no permitidos");
    next();
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};