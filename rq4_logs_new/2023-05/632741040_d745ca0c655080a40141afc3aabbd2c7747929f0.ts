// import bcrypt from 'bcryptjs'
import { genSaltSync, hashSync } from 'bcrypt-ts'
import { IUser } from '../module/user/model'

const users: IUser[] = [
  {
    name: 'Admin User',
    username: 'admin',
    email: 'admin@example.com',
    password: hashSync('123456', genSaltSync(10)),
    role: 'admin',
  },
  {
    name: 'John Doe',
    username: 'jhon',
    email: 'john@example.com',
    password: hashSync('123456', genSaltSync(10)),
    role: 'user',
  },
  {
    name: 'Jane doe',
    username: 'jane',
    email: 'jane@example.com',
    password: hashSync('123456', genSaltSync(10)),
    role: 'user',
  },
]

export default users