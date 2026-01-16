import { Sequelize } from "sequelize-typescript";
import ProductModel from "../../../infrastructure/product/repository/sequelize/product.model";
import ProductRepository from "../../../infrastructure/product/repository/sequelize/product.repository";
import CreateProductUseCase from "./create.product.usecase";

let sequelize: Sequelize;

beforeEach(async () => {
    sequelize = new Sequelize({
        dialect: "sqlite",
        storage: ":memory:",
        logging: false,
        sync: {force: true}
    });

    sequelize.addModels([ProductModel]);
    await sequelize.sync();
});

afterEach(async () => {
    await sequelize.close();
});

describe("product A create usecase unit test", () => {
    const input = {
        type: "a",
        name: "product 1",
        price: 150.0
    };

    it("should create a product", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        const output = await productCreateUseCase.execute(input);

        expect(output).toEqual({
            id: expect.any(String),
            name: input.name,
            price: input.price
        });
    });

    it("should throw an error when product's name is empty", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        input.name = "";
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Name is required"
        );

        input.name = "Product 1";
    });

    it("should thrown an error when product's price is less than zero", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);
        
        input.price = -10;
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Price must be greater than zero"
        );
    });
});

describe("product B create usecase unit test", () => {
    const input = {
        type: "b",
        name: "product 1",
        price: 150.0
    };

    it("should create a product", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        const output = await productCreateUseCase.execute(input);

        expect(output).toEqual({
            id: expect.any(String),
            name: input.name,
            price: 300.0
        });
    });

    it("should throw an error when product's name is empty", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        input.name = "";
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Name is required"
        );

        input.name = "Product 1";
    });

    it("should thrown an error when product's price is less than zero", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);
        
        input.price = -10;
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Price must be greater than zero"
        );
    });
});

describe("product unknown create usecase unit test", () => {
    const input = {
        type: "c",
        name: "product 1",
        price: 150.0
    };

    it("should thrown a product type not supported error", async () => {
        const productRepository = new ProductRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "product type not supported"
        );
    });
});