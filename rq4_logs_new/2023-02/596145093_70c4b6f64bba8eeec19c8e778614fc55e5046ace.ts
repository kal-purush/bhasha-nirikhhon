import { ReadUsersController } from "../../presentation/controller/read-users/read-users";
import { ReadUsersAdapter } from '../../data/useCases/read-users/read-users-adapter';
import { ReadUsersRepositoryAdapter } from "../../data/repositories/read-users/read-users";
import { Mockend } from "../../utils/mockend-utils";

export const makeReadUsersController = (): ReadUsersController => {
  const mockend = new Mockend();
  const readUsersRepository = new ReadUsersRepositoryAdapter(mockend);
  const readUsers = new ReadUsersAdapter(readUsersRepository);
  return new ReadUsersController(readUsers);
} 