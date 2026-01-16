import { MockendInterface } from '../../../utils/mockend-utils';
import { Product } from "../../../domain/models/product";
import { User } from "../../../domain/models/user";
import { ReadProductsRepositoryAdapter } from './read-products';

const fakeProducts = [
  {
    "id": 1,
    "name": "explicabo alias hic reprehenderit deleniti quos id reprehenderit consequuntur ipsam iure voluptatem ea culpa excepturi ducimus repudiandae ab",
    "price": 6945
  },
  {
    "id": 2,
    "name": "nostrum veritatis reprehenderit repellendus vel numquam soluta ex inventore ex",
    "price": 2435
  }
]

const makeMockend = (): MockendInterface => {
  class MockendMock implements MockendInterface {
    fetch(type: "users" | "products"): Promise<User[] | Product[]> {
      return new Promise(resolve => resolve(fakeProducts));
    }
  }

  return new MockendMock();
}

interface TypesSut {
  sut: ReadProductsRepositoryAdapter
  mockend: MockendInterface
}

const makeSut = (): TypesSut => {
  const mockend = makeMockend();
  const sut = new ReadProductsRepositoryAdapter(mockend);

  return {
    sut,
    mockend
  }
}

describe("ReadProducts Repository Adapter", () => {
  it("Should call mockendGet with Products", async () => {
    const { sut, mockend } = makeSut();

    const mockendGet = jest.spyOn(mockend, "fetch");
    await sut.read();

    expect(mockendGet).toBeCalledWith("products")
  })

  it("Should return the correct value", async () => {
    const { sut } = makeSut();

    const products = await sut.read();

    expect(products).toEqual(fakeProducts);
  })
})