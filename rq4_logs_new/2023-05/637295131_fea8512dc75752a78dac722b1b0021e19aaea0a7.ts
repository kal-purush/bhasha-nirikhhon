import bcrypt from "bcrypt";
import { ROLE_CONSUMER, UserModel } from "../user/schemas/user.schema";

export const prepareAccount = async () => {
  const admin = await UserModel.findOne({ username: "admin@gmail.com" });
  if (!admin) {
    const passwordHash = bcrypt.hashSync("123456", bcrypt.genSaltSync());

    return await UserModel.create({ username: "admin@email.com", password: passwordHash, role: ROLE_CONSUMER.ADMIN, displayName: "Admin" });
  }
  return;
};