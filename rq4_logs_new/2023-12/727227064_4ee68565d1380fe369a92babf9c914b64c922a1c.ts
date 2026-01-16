import { Role } from "../enums/role.enum";
import { UserEntity } from "../user/entity/user.entity";

export const userEntityList: UserEntity[] = [{
  id: 1,
  name: 'John',
  email: 'john@example.com',
  password: '123456',
  birthday: new Date('1994-11-05'),
  role: Role.Admin,
  createdat: new Date(),
  updatedat: new Date()
}, {
  id: 2,
  name: 'jose',
  email: 'jose@example.com',
  password: '123456',
  birthday: new Date('1994-11-05'),
  role: Role.User,
  createdat: new Date(),
  updatedat: new Date()
}]