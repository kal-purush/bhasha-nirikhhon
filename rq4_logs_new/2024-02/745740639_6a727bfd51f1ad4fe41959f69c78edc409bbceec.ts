import { beforeAll, describe, expect, it } from "vitest";
import { CreateCourseUseCase } from "./create-course";
import { InMemoryCoursesRepository } from "../../../../../tests/repositories/in-memory-courses-repository";
import { InvalidParametersError } from "@/core/errors/invalid-parameters";

let inMemoryCoursesRepository: InMemoryCoursesRepository
let sut: CreateCourseUseCase

describe('Create course', () => {
  beforeAll(() => {
    inMemoryCoursesRepository = new InMemoryCoursesRepository()
    sut = new CreateCourseUseCase(inMemoryCoursesRepository)
  })

  it('should be able to create a new course', async () => {
    const course = await sut.execute({
      course_name: 'Course Test',
      description: 'Description test',
      instructor_id: 'instructor-1'
    })

    expect(course.course.course_name).toEqual('Course Test')
  })

  it('should not be able to create a new course with missing information', async () => {
    expect(
      sut.execute({
        course_name: '',
        description: 'Description test',
        instructor_id: 'instructor-1'
      })
    ).rejects.toEqual(new InvalidParametersError())
  })
})