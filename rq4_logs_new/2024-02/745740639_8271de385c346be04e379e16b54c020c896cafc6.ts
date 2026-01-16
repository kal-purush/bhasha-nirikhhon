import { Student, StudentProps } from '@/domain/educational/enterprise/entities/student'
import { PrismaStudentMapper } from '@/infra/database/mappers/prisma-student-mapper'
import { prisma } from '@/infra/database/prisma'
import { faker } from '@faker-js/faker'

export function makeUser() {
  const student = Student.create(
    {
      first_name: faker.person.firstName(),
      last_name: faker.person.lastName(),
      email: 'john@doe.com',
      document: faker.person.middleName(),
      password: 'teste123',
      phone_number: faker.person.gender(),
      role: 'STUDENT'
    }
  )

  return student
}

export class UserFactory {

  async makePrismaStudent(): Promise<Student> {
    const student = makeUser()
    await prisma.user.create({
      data: PrismaStudentMapper.toPrisma(student),
    })

    return student
  }
}