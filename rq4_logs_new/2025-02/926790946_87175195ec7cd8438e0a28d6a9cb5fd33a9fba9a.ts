import { InternalServerError, ResetPasswordServiceError } from "@/lib/CustomErrors";
import Hasher from "@/lib/Hasher";
import Mailer from "@/lib/Mailer";
import TokenService from "@/lib/TokenService";
import ResetPasswordRepository from "@/repository/ResetPasswordRepository";
import UserRepository from "@/repository/UserRepository";
import ResetPasswordService from "@/services/auth/ResetPasswordService";
import { NextApiRequest, NextApiResponse } from "next";

const userRepository = new UserRepository();
const resetPasswordRepository = new ResetPasswordRepository();
const tokenService = new TokenService();
const hasher = new Hasher();
const mailer = new Mailer();
const resetPasswordService = new ResetPasswordService(
  userRepository,
  resetPasswordRepository,
  tokenService,
  hasher,
  mailer,
);

export default async function RequestResetPassword(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    return res.status(405).json({ message: "Method Not Allowed." });
  }

  try {
    const { email } = req.body;

    await resetPasswordService.requestResetPassword(email);

    return res.status(200).json({ message: "Email sent" });
  } catch (error) {
    if (error instanceof ResetPasswordServiceError || error instanceof InternalServerError) {
      res
        .status(error.statusCode)
        .json({ message: error.message, action: error.action, isPublicError: error.isPublicError });
    }
  }
}