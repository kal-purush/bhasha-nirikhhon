import { InternalServerError, LoginServiceError, TokenServiceError } from "@/lib/CustomErrors";
import Hasher from "@/lib/Hasher";
import TokenService from "@/lib/TokenService";
import AuthRepository from "@/repository/AuthRepository";
import UserRepository from "@/repository/UserRepository";
import { LoginService } from "@/services/auth/LoginService";
import { NextApiRequest, NextApiResponse } from "next";

const userRepository = new UserRepository();
const authRepository = new AuthRepository();
const hasher = new Hasher();
const tokenService = new TokenService();
const loginService = new LoginService(userRepository, authRepository, hasher, tokenService);

export default async function login(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    return res.status(403).json({ message: "Method Not Allowed." });
  }

  try {
    const { email, password } = req.body;

    await loginService.login(email, password);

    return res.status(200).json({ message: "Login Successfull." });
  } catch (error) {
    if (
      error instanceof LoginServiceError ||
      error instanceof TokenServiceError ||
      error instanceof InternalServerError
    ) {
      return res
        .status(error.statusCode)
        .json({ message: error.message, action: error.action, isPublicError: error.isPublicError });
    }
  }
}