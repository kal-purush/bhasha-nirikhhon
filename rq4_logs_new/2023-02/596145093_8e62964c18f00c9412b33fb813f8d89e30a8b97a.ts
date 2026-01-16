import { ReadUsersRepositoryAdapter } from "./read-users";
import { MockendInterface } from '../../../utils/mockend-utils';
import { Product } from "../../../domain/models/product";
import { User } from "../../../domain/models/user";

const fakeUsers = [
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
]

const makeMockend = (): MockendInterface => {
  class MockendMock implements MockendInterface {
    fetch(type: "users" | "products"): Promise<User[] | Product[]> {
      return new Promise(resolve => resolve(fakeUsers));
    }
  }

  return new MockendMock();
}

interface TypesSut {
  sut: ReadUsersRepositoryAdapter
  mockend: MockendInterface
}

const makeSut = (): TypesSut => {
  const mockend = makeMockend();
  const sut = new ReadUsersRepositoryAdapter(mockend);

  return {
    sut,
    mockend
  }
}

describe("ReadUsers Repository Adapter", () => {
  it("Should call mockendGet with users", async () => {
    const { sut, mockend } = makeSut();

    const mockendGet = jest.spyOn(mockend, "fetch");
    await sut.read();

    expect(mockendGet).toBeCalledWith("users")
  })

  it("Should return the correct value", async () => {
    const { sut } = makeSut();

    const users = await sut.read();

    expect(users).toEqual(fakeUsers);
  })
})