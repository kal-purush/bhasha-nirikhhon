import { mockendGet } from './mockend-utils';

const PRODUCTS_URL = "https://mockend.com/juunegreiros/BE-test-api/products";
const USERS_URL = "https://mockend.com/juunegreiros/BE-test-api/users";

describe("Mockend Utils", () => {
  it("Should return an array of Products when receive the product url", async () => {
    const products = await mockendGet(PRODUCTS_URL);

    expect(products).toBeTruthy();
    expect(products[0]).toHaveProperty("id");
    expect(products[0]).toHaveProperty("name");
    expect(products[0]).toHaveProperty("price");
  });

  it("Should return an array of Users when receive the user url", async () => {
    const users = await mockendGet(USERS_URL);

    expect(users).toBeTruthy();
    expect(users[0]).toHaveProperty("id");
    expect(users[0]).toHaveProperty("name");
    expect(users[0]).toHaveProperty("tax");
  });
})