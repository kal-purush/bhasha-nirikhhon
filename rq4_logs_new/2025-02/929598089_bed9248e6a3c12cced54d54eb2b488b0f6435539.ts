import api from "../axios";
import { User } from "../types/api";

export class UserService {
  static async getProfile(): Promise<User> {
    const response = await api.get<User>("/users/profile");
    return response.data;
  }

  static async updateProfile(data: Partial<User>): Promise<User> {
    const response = await api.put<User>("/users/profile", data);
    return response.data;
  }

  static async updatePassword(
    currentPassword: string,
    newPassword: string
  ): Promise<void> {
    await api.put("/users/password", {
      currentPassword,
      newPassword,
    });
  }
}