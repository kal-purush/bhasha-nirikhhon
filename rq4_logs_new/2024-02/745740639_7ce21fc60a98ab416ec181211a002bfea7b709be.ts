import { beforeAll, describe, expect, it } from "vitest";
import { InMemoryUsersRepository } from "../../../../../tests/repositories/in-memory-users-repository";
import { EditProfileUseCase } from "./edit-profile";
import { makeUser } from "../../../../../tests/factories/make-user";
import { UserNotFoundError } from "@/core/errors/user-not-found";

let inMemoryUsersRepository: InMemoryUsersRepository
let sut: EditProfileUseCase

describe("Edit profile use case", () => {
  beforeAll(() => {
    inMemoryUsersRepository = new InMemoryUsersRepository()
    sut = new EditProfileUseCase(inMemoryUsersRepository)
  })

  it('should be able to edit a profile of an existent user', async () => {
    const student = makeUser()
    inMemoryUsersRepository.items.push(student)
    const updatedUser = await sut.execute({
      id: student.id,
      first_name: 'test-edit',
      last_name: 'test-edit-lastname',
    })

    expect(updatedUser.user.first_name).toEqual('test-edit')
  })

  it('should not be able to edit a profile of non existent user', async () => {
    expect(sut.execute({
      id: '1',
      first_name: 'test-edit',
      last_name: 'test-edit-lastname',
    })).rejects.toEqual(new UserNotFoundError())
  })
})