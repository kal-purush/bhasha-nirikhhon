import { Request, Response, NextFunction } from "express";
import AuthService from "../../services/auth/auth.service";

const authService = new AuthService();

export const createAdmin = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const requestPayload = req.body;
    const response = await authService.signUp(requestPayload);

    if (!response) {
      throw new Error("An unknown error occured while creating your admin account");
    }
    
    res.json({
      message: "Admin created",
      success: true
    });
  } catch (error) {
    next(error);
  }
}

export const login = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const requestPayload = req.body;
    const loginResponse = await authService.signIn(requestPayload);

    if (!loginResponse.success) {
      throw new Error("An unknown error occured while signing you into your admin account");
    }

    res.json({
      message: "Login successful",
      success: true,
      access_token: loginResponse.access_token
    });
  } catch (error) {
    next(error);
  }
}