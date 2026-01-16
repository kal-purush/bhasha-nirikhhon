import { Sequelize } from "sequelize-typescript";
import Product from "../../../domain/product/entity/product";
import ProductModel from "../../../infrastructure/product/repository/sequelize/product.model";
import ProductRepository from "../../../infrastructure/product/repository/sequelize/product.repository";
import FindProductUseCase from "./find.product.usecase";

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

const product = new Product("123", "Product", 123.50);
const productRepository = new ProductRepository();



describe("Product find usecase unit tests", () => {
    it("should get a product", async () => {       
        await productRepository.create(product); 
        const usecase = new FindProductUseCase(productRepository);

        const input = {
            id: "123"
        }

        const output = {
            id: "123",
            name: "Product",
            price: 123.50
        };

        const result = await usecase.execute(input);
        expect(result).toEqual(output);
    });

    it("should get an exception product not found", () => {        
        const usecase = new FindProductUseCase(productRepository);

        const input = {
            id: "not_a_product"
        }

        expect(() => {
            return usecase.execute(input)
        }).rejects.toThrow("product not found");
    });
});