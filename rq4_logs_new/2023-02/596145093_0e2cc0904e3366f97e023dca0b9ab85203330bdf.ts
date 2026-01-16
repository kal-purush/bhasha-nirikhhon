import { User } from "../../../domain/models/user";
import { ReadUsersRepository } from "../../protocols/read-users";
import { ReadUsersAdapter } from "./read-users-adapter";

interface SutTypes {
  sut: ReadUsersAdapter,
  readUsersRepositoryStub: ReadUsersRepository
}

const fakeUsers: User[] = [
  {
    "id": 1,
    "name": "cvRhuZicvV",
    "tax": 79
  },
  {
    "id": 2,
    "name": "P5hBDBonm3",
    "tax": 121
  }
];

const makeRepository = (): ReadUsersRepository => {
  class ReadUsersStub implements ReadUsersRepository {
    read(): Promise<User[]> {
      return new Promise(resolve => resolve(fakeUsers));
    }
  }

  return new ReadUsersStub();
}

const makeSut = (): SutTypes => {
  const readUsersRepositoryStub = makeRepository();
  const sut = new ReadUsersAdapter(readUsersRepositoryStub);

  return {
    sut,
    readUsersRepositoryStub
  }
}

describe("ReadUsers Adapter", () => {
  it("Should throw if Repository throws", async () => {
    const { sut, readUsersRepositoryStub } = makeSut();

    jest.spyOn(readUsersRepositoryStub, "read").mockReturnValueOnce(new Promise((_r, reject) => reject(new Error())));
    const promise = sut.read();

    expect(promise).rejects.toThrow();
  })

  it("Should return the correct values on success", async () => {
    const { sut } = makeSut();

    const response = await sut.read();

    expect(response).toBe(fakeUsers);
  })
})