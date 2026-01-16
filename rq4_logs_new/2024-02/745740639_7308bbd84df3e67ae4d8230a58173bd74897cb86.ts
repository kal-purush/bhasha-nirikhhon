import { beforeEach, describe, expect, it } from "vitest";
import { GetProfileByIdUseCase } from "./get-profile-by-id";
import { InMemoryUsersRepository } from "../../../../../tests/repositories/in-memory-users-repository";
import { makeUser } from "../../../../../tests/factories/make-user";
import { UserNotFoundError } from "@/core/errors/user-not-found";

let inMemoryUsersRepository: InMemoryUsersRepository
let sut: GetProfileByIdUseCase

describe('Get Profile by Id', () => {
  beforeEach(() => {
    inMemoryUsersRepository = new InMemoryUsersRepository()
    sut = new GetProfileByIdUseCase(inMemoryUsersRepository)
  })

  it('should be able to get user profile', async () => {
    const payload = makeUser()
    const user = inMemoryUsersRepository.items.push(payload)

    const profile = await sut.execute({ id: payload.id })

    expect(profile.user.id).toEqual(payload.id)
  })

  it('should not be able to get user profile if the id doesnot exists', async () => {
    const payload = makeUser()
    const user = inMemoryUsersRepository.items.push(payload)

    expect(
      sut.execute({ id: 'teste' })
    ).rejects.toEqual(new UserNotFoundError())
  })
})