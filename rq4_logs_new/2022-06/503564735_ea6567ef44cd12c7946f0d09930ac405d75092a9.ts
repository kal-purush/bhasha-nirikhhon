import { Sequelize } from "sequelize-typescript";
import Product from "../../../domain/product/entity/product";
import ProductModel from "../../../infrastructure/product/repository/sequelize/product.model";
import ProductRepository from "../../../infrastructure/product/repository/sequelize/product.repository";
import { ListProductUseCase } from "./list.product.usecase";

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

describe("product list use case unit tests", () => {
    const productOne = new Product("123", "Product 123", 156.20);
    const productTwo = new Product("456", "Product 456", 77.88);
    
    it("should show a product list", async () => {
        const productRepository = new ProductRepository();
        productRepository.create(productOne);
        productRepository.create(productTwo);
        
        const usecase = new ListProductUseCase(productRepository);
        
        const output = await usecase.execute({});

        expect(output.products.length).toBe(2);
        expect(output.products[0].id).toBe("123");
        expect(output.products[0].name).toBe("Product 123");
        expect(output.products[0].price).toBe(156.20);
        expect(output.products[1].id).toBe("456");
        expect(output.products[1].name).toBe("Product 456");
        expect(output.products[1].price).toBe(77.88);
    });
});