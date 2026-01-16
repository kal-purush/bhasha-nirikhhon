import { Product } from "../../domain/models/product";
import { ReadProducts } from "../../domain/useCases/read-products";
import { ServerError } from "../errors/server-error";
import { ReadProductsController } from "./read-products";

interface ControllerTypes {
  sut: ReadProductsController
  readProducts: ReadProducts
}

const makeReadProducts = (): ReadProducts => {
  class ReadProductsStub implements ReadProducts {
    async read(): Promise<Product[]> {
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

      return new Promise(resolve => resolve(fakeProducts));
    }
  }

  return new ReadProductsStub();
}

const makeController = (): ControllerTypes => {
  const readProducts = makeReadProducts();
  const sut = new ReadProductsController(readProducts);

  return {
    sut,
    readProducts
  }
}

describe("ReadProductsController", () => {
  it('Should call ReadProducts', async () => {
    // given
    const { sut, readProducts } = makeController();
    
    // when
    const called = jest.spyOn(readProducts, "read");
    sut.handle({ body: '' })

    // then
    expect(called).toHaveBeenCalled();
  })

  it('Should return 500 if ReadProducts throws', async () => {
    // given
    const { sut, readProducts } = makeController();
    
    // when
    jest.spyOn(readProducts, "read").mockImplementationOnce(() => {
      throw new Error();
    })
    const httpResponse = await sut.handle({ body: "" });

    // then
    expect(httpResponse.statusCode).toBe(500);
    expect(httpResponse.body).toEqual(new ServerError());
  })

  it('Should return 200 and the corrects Products', async () => {
    // given
    const { sut } = makeController();
    
    // when
    const httpResponse = await sut.handle({ body: "" });

    // then
    expect(httpResponse.statusCode).toBe(200);
    expect(httpResponse.body).toEqual([
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
    ])
  })
})