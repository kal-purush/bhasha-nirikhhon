import { Role } from "../enums/role.enum";
import { CreateUserDTO } from "../user/dto/create-user-dto";

export const createUserDTO: CreateUserDTO = {
  name: 'John',
  email: 'john@example.com',
  password: '123456',
  birthday: '1994-11-05',
  role: Role.User
}