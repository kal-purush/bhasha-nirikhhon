import { catalogBatchProcess } from ".";

jest.mock("../../db/withPgConnection", () => ({
  withPgConnection: (handler) => (...args) =>
    handler({ query: async () => ({ rows: [{ id: 1 }] }) }, ...args),
}));

jest.mock("aws-sdk", () => ({
  SNS: class SNS {
    constructor() {
      return {
        publish: () => ({
          promise: () => {},
        }),
      };
    }
  },
}));

const validProduct = {
  count: 1,
  description: "Description",
  price: 10,
  title: "Toy",
  src: "url",
};
const invalidProduct = { ...validProduct, price: "Too expensive" };

describe("#catalogBatchProcess", () => {
  it("should generate error message if product is not valid", async () => {
    const consoleLogSpy = jest.spyOn(console, "log");

    await catalogBatchProcess({
      Records: [{ body: JSON.stringify(invalidProduct) }],
    });

    expect(consoleLogSpy).toHaveBeenCalledWith(
      "DB error:",
      `Product ${invalidProduct.title} is not valid`
    );
  });

  it("should generate success message if product is valid", async () => {
    const consoleLogSpy = jest.spyOn(console, "log");

    await catalogBatchProcess({
      Records: [{ body: JSON.stringify(validProduct) }],
    });

    expect(consoleLogSpy).toHaveBeenCalledWith(
      `Product ${validProduct.title} was created`
    );
  });
});