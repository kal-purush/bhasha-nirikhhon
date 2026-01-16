import UserRepository from "@/repository/UserRepository";
import Hasher from "@/lib/Hasher";
import { NextApiRequest, NextApiResponse } from "next";
import RegisterService from "@/services/auth/RegisterService";
import { RegisterServiceError, UserValidationsError } from "@/lib/CustomErrors";

const userRepository = new UserRepository();
const hasher = new Hasher();
const registerService = new RegisterService(userRepository, hasher);

export default async function register(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method Not Allowed." });
  }

  try {
    const { name, email, password, passwordConfirmation } = req.body;

    await registerService.register({ name, email, password, passwordConfirmation });

    return res.status(201).json({ message: "User Created." });
  } catch (error) {
    if (error instanceof UserValidationsError || error instanceof RegisterServiceError) {
      return res
        .status(error.statusCode)
        .json({ error: error.message, action: error.action, isPublicError: error.isPublicError });
    }

    return res.status(500).json({ error: "Internal Server Error." });
  }
}