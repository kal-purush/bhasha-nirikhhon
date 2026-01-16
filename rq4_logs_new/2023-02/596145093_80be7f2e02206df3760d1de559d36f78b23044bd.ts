import { Product } from "../../../domain/models/product";
import { ReadProductsRepository } from "../../protocols/read-products";
import { ReadProductsAdapter } from "./read-products-adapter";

interface SutTypes {
  sut: ReadProductsAdapter,
  readProductsRepositoryStub: ReadProductsRepository
}

const fakeProducts: Product[] = [
  {
    "id": 1,
    "name": "explicabo alias hic reprehenderit deleniti quos id reprehenderit consequuntur ipsam iure voluptatem ea culpa excepturi ducimus repudiandae ab",
    "price": 6945
  },
  {
    "id": 2,
    "name": "nostrum veritatis reprehenderit repellendus vel numquam soluta ex inventore ex",
    "price": 2435
  },
];

const makeRepository = (): ReadProductsRepository => {
  class CreateBudgeStub implements ReadProductsRepository {
    read(): Promise<Product[]> {
      return new Promise(resolve => resolve(fakeProducts));
    }
  }

  return new CreateBudgeStub();
}

const makeSut = (): SutTypes => {
  const readProductsRepositoryStub = makeRepository();
  const sut = new ReadProductsAdapter(readProductsRepositoryStub);

  return {
    sut,
    readProductsRepositoryStub
  }
}

describe("CreateBudget Adapter", () => {
  it("Should throw if Repository throws", async () => {
    const { sut, readProductsRepositoryStub } = makeSut();

    jest.spyOn(readProductsRepositoryStub, "read").mockReturnValueOnce(new Promise((_r, reject) => reject(new Error())));
    const promise = sut.read();

    expect(promise).rejects.toThrow();
  })

  it("Should return the correct values on success", async () => {
    const { sut } = makeSut();

    const response = await sut.read();

    expect(response).toBe(fakeProducts);
  })
})