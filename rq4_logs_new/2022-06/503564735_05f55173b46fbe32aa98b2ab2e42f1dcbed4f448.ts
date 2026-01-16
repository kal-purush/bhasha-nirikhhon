import CreateProductUseCase from "./create.product.usecase";

const MockRepository = () => { 
    return {
        create: jest.fn(),
        find: jest.fn(),
        findAll: jest.fn(),
        update: jest.fn(),
    }
};

describe("product A create usecase unit test", () => {
    const input = {
        type: "a",
        name: "product 1",
        price: 150.0
    };

    it("should create a product", async () => {
        const productRepository = MockRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        const output = await productCreateUseCase.execute(input);

        expect(output).toEqual({
            id: expect.any(String),
            name: input.name,
            price: input.price
        });
    });

    it("should throw an error when product's name is empty", async () => {
        const productRepository = MockRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        input.name = "";
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Name is required"
        );

        input.name = "Product 1";
    });

    it("should thrown an error when product's price is less than zero", async () => {
        const productRepository = MockRepository();
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
        const productRepository = MockRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        const output = await productCreateUseCase.execute(input);

        expect(output).toEqual({
            id: expect.any(String),
            name: input.name,
            price: 300.0
        });
    });

    it("should throw an error when product's name is empty", async () => {
        const productRepository = MockRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);

        input.name = "";
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Name is required"
        );

        input.name = "Product 1";
    });

    it("should thrown an error when product's price is less than zero", async () => {
        const productRepository = MockRepository();
        const productCreateUseCase = new CreateProductUseCase(productRepository);
        
        input.price = -10;
        await expect(productCreateUseCase.execute(input)).rejects.toThrow(
            "Price must be greater than zero"
        );
    });
});