import { UserServiceError } from "@/lib/CustomErrors";
import UserRepository from "@/repository/UserRepository";
import UserService from "@/services/UserService";
import { NextApiRequest, NextApiResponse } from "next";

const userRepository = new UserRepository();
const userService = new UserService(userRepository);

export default async function findUser(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ message: "Method Not Allowed" });
  }

  try {
    const { id } = req.query;

    console.log(id);

    const user = await userService.findUserById(id as string);

    return res.status(200).json(user);
  } catch (error) {
    if (error instanceof UserServiceError) {
      return res.status(error.statusCode).json(error);
    }
  }
}