import { UsersRepository } from "@/domain/educational/application/repositories/users-repository";
import { Student } from "@/domain/educational/enterprise/entities/student";
import { UserWithoutPassword } from "@/domain/educational/enterprise/entities/value-objects/user-without-password";
import { prisma } from "@/infra/database/prisma";
import { PrismaStudentMapper } from "../../mappers/prisma-student-mapper";
import { Instructor } from "@/domain/educational/enterprise/entities/instructor";

export class PrismaUsersRepository implements UsersRepository {
  async create(data: Student | Instructor): Promise<void> {
    const userData = PrismaStudentMapper.toPrisma(data);
    await prisma.user.create({
      data: userData
    })
  }

  async findByEmail(email: string): Promise<UserWithoutPassword | null> {
    const user = await prisma.user.findUnique({
      where: {
        email
      }
    })

    if (!user) return null

    return UserWithoutPassword.create({
      id: user.id,
      first_name: user.first_name,
      last_name: user.last_name,
      email: user.email,
      document: user.document,
      phone_number: user.phone_number,
      created_at: user.created_at,
      updated_at: user.updated_at
    });
  }

  async findByDocument(document: string): Promise<UserWithoutPassword | null> {
    const user = await prisma.user.findUnique({
      where: {
        document
      }
    })

    if (!user) return null

    return UserWithoutPassword.create({
      id: user.id,
      created_at: user.created_at,
      document: user.document,
      email: user.email,
      first_name: user.first_name,
      last_name: user.last_name,
      phone_number: user.phone_number,
      updated_at: user.updated_at
    });
  }
}