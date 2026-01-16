import { UserServiceError } from "@/lib/CustomErrors";
import { IUserRepository } from "@/repository/UserRepository";

export default class UserService {
  constructor(private readonly userRepository: IUserRepository) {}

  async findUserById(id: string) {
    try {
      const user = await this.userRepository.findUserById(id);
      if (!user) {
        throw new UserServiceError("User not found.", "Check the id provided", 404, false);
      }

      return user;
    } catch (error) {
      console.error("Error during find user by id: ", error);

      if (error instanceof UserServiceError) {
        throw error;
      }
    }
  }
}