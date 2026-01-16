import * as moment from "moment";
import { v4 as uuid } from "uuid";
import { IProduct } from "../../common/models/product";
import { IProductsDataSource } from "./products-data-source";

const products: IProduct[] = [
  {
    id: "faa65af2-ac6d-4404-9d9d-7423f04eb740",
    name: "Poc Chuc Torta",
    description: "Is a Poc Chuc Torta",
    price: 25,
    createdAt: 1548000000,
    updatedAt: 1548000000
  },
  {
    id: "faa65af2-ac6d-4404-9d9d-7423f04eb740",
    name: "Shrimp Torta",
    description: "Is a Poc Chuc Torta",
    price: 25,
    createdAt: 1548000000,
    updatedAt: 1548000000
  }
];

const find = (query?: any): Promise<IProduct[]> => {
  if (query) {
    const [key]: string[] = Object.keys(query);
    return Promise.resolve(
      products.filter((user: IProduct) => user[key] === query[key])
    );
  }

  return Promise.resolve(products);
};

const save = (product: IProduct): Promise<IProduct> => {
  product.id = uuid();
  const result: IProduct = {
    ...product,
    createdAt: moment()
      .utc()
      .unix(),
    updatedAt: moment()
      .utc()
      .unix()
  };

  products.push(result);

  return Promise.resolve(result);
};

const update = (product: IProduct): Promise<IProduct> => {
  const index: number = products.findIndex(
    (p: IProduct) => p.id === product.id
  );

  products[index] = { ...products[index], ...product };

  return Promise.resolve(products[index]);
};

export const productsMemoryDataSource: IProductsDataSource = {
  find,
  save,
  update
};