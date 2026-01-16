import { CreateUserBodySchema } from '@/infra/http/controllers/create-user.controller'
import { UpdateProfileBodySchema } from '@/infra/http/controllers/update-profile.controller'
import { User } from '@prisma/client'

export interface IUserRepository {
  create(user: CreateUserBodySchema): Promise<void>
  getOneByEmail(email: string): Promise<User | null>
  updatePassword(password: string, userId: string): Promise<void>
  updateProfile(profile: UpdateProfileBodySchema, userId: string): Promise<void>
  updateAvatar(avatarUrl: string, userId: string): Promise<void>
}