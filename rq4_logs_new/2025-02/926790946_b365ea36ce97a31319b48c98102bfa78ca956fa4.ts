import { InternalServerError, LogoutServiceError } from "@/lib/CustomErrors";
import Hasher from "@/lib/Hasher";
import TokenService from "@/lib/TokenService";
import AuthRepository from "@/repository/AuthRepository";
import UserRepository from "@/repository/UserRepository";
import AuthService from "@/services/AuthService";
import { NextApiRequest, NextApiResponse } from "next";

const userRepository = new UserRepository();
const authRepository = new AuthRepository();
const hasher = new Hasher();
const tokenService = new TokenService();
const authService = new AuthService(userRepository, authRepository, hasher, tokenService);

export default async function logout(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    return res.status(405).json({ message: "Internal Server Error." });
  }

  try {
    const { email } = req.body;

    await authService.logout(email);

    res.setHeader(
      "Set-Cookie",
      "sessionToken=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Strict",
    );

    return res.status(200).json({ message: "Logout Successfull" });
  } catch (error) {
    if (error instanceof LogoutServiceError || error instanceof InternalServerError) {
      return res
        .status(error.statusCode)
        .json({ message: error.message, action: error.action, isPublicError: error.isPublicError });
    }
  }
}