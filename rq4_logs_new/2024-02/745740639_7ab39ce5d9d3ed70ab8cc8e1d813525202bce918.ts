import { CoursesRepository } from "@/domain/educational/application/repositories/courses-repository";
import { Course } from "@/domain/educational/enterprise/entities/course";
import { Course as CoursePrisma } from "@prisma/client";
import { prisma } from "../../prisma";

export class PrismaCoursesRepository implements CoursesRepository {
  async create(data: Course): Promise<CoursePrisma> {
    const course = await prisma.course.create({
      data
    })

    return course
  }

  async findById(id: string): Promise<CoursePrisma | null> {
    const course = await prisma.course.findFirst({
      where: {
        id
      }
    })

    return course
  }

  async findManyByInstructorId(instructorId: string): Promise<CoursePrisma[]> {
    const course = await prisma.course.findMany({
      where: {
        instructor_id: instructorId
      }
    })

    return course
  }

}