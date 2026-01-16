import { app, sequelize } from '../express';
import request from "supertest";
import Product from '../../../domain/product/entity/product';
import ProductRepository from '../../product/repository/sequelize/product.repository';

describe("E2E test for product", () => {
    beforeEach(async () => {
        await sequelize.sync({force: true});
    });

    afterAll(async () => {
        await sequelize.close();
    });    

    it("should list all products", async () => {
        const productOne = new Product("123", "Product A", 120.0);
        const productTwo = new Product("456", "Product B", 55.0);
        const productRepository = new ProductRepository();
        await productRepository.create(productOne);
        await productRepository.create(productTwo);

        const listResponse = await request(app).get("/product").send();
        expect(listResponse.status).toBe(200);
        expect(listResponse.body.products.length).toBe(2);
        const resultOne = listResponse.body.products[0];
        expect(resultOne.id).toBe("123");
        expect(resultOne.name).toBe("Product A");
        expect(resultOne.price).toBe(120.0);
        const resultTwo = listResponse.body.products[1];
        expect(resultTwo.id).toBe("456");
        expect(resultTwo.name).toBe("Product B");
        expect(resultTwo.price).toBe(55.0);
    })

});