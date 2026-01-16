import User from "../modules/user/user.schema";
import { CreateUserDto } from "../modules/user/user.types";
import mongoose from "mongoose";

/**
 * Add a new user to the database.
 * This service can be reused across modules.
 * @param userData - Data for creating a new user
 */
export const addUser = async (userData: CreateUserDto) => {
  const user = new User({
    ...userData,
    auth_id: new mongoose.Types.ObjectId(userData.auth_id),
    created_at: new Date(),
    updated_at: new Date(),
  });

  return await user.save();
};