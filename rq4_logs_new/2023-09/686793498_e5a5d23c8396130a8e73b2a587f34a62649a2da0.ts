import { Router } from "express";
import Paths from "../util/constants/Paths";
import AuthController from "./AuthController";

// **** Routes **** //
export const authRouter = Router();

// Register user
authRouter.post(
  Paths.Auth.Register,
  //validate(['user', User.isUser]), TODO
  AuthController.register
);

// Login user
authRouter.post(
  Paths.Auth.Login,
  //validate('email', 'password'),
  AuthController.login,
);

// Get current user
authRouter.get(
  Paths.Auth.Me,
  AuthController.me,
);

// Logout user
authRouter.get(
  Paths.Auth.Logout,
  AuthController.logout,
);