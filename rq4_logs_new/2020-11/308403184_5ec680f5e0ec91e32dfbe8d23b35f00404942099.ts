import { withCorsHeaders } from "../../utils/withCorsHeaders";
import { APIGatewayProxyResult } from "aws-lambda";

const newProduct = {
  count: 1,
  description: "Description",
  price: 10,
  title: "Toy",
  src: "url",
};

describe("#createProduct", () => {
  beforeEach(() => {
    jest.resetModules();
  });

  it("should return error if product is not valid", async () => {
    jest.mock("../../db/withPgConnection", () => ({
      withPgConnection: (handler) => (...args) =>
        handler({ query: async () => ({ rows: [] }) }, ...args),
    }));
    const { createProduct } = require(".");

    const response = await createProduct({
      body: JSON.stringify({ ...newProduct, count: "not valid count" }),
    });

    expect(response).toMatchObject(
      withCorsHeaders({ statusCode: 400 } as APIGatewayProxyResult)
    );
  });

  it("should return product if it was successfully created", async () => {
    jest.mock("../../db/withPgConnection", () => ({
      withPgConnection: (handler) => (...args) =>
        handler({ query: async () => ({ rows: [{}] }) }, ...args),
    }));
    const { createProduct } = require(".");

    const response = await createProduct({
      body: JSON.stringify(newProduct),
    });

    expect(response).toMatchObject(
      withCorsHeaders({
        statusCode: 200,
        body: JSON.stringify({}),
      })
    );
  });
});