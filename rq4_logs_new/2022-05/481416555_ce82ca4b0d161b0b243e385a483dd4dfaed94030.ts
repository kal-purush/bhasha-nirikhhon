import { Sequelize } from "sequelize-typescript";
import CustomerRepository from "../../../domain/customer/repository/customer.repository";
import CustomerModel from "../../../infrastructure/customer/repository/sequelize/customer.model";
import CreateCustomerUseCase from "./create.customer.usecase";

const input = {
    name: "John",
    address: {
        street: "Street",
        number: 123,
        zip: "zip",
        city: "city",
    },
};

describe("Integration test create customer use case", () => {
    let sequelize: Sequelize;
    beforeEach(async() => {
        sequelize = new Sequelize({
            dialect: "sqlite",
            storage: ":memory:",
            logging: false,
            sync: { force: true },
        });
        sequelize.addModels([CustomerModel]);
        await sequelize.sync();
    });

    afterEach(async () => {
        await sequelize.close();
    });

    it("should create a customer", async () => {
        const customerRepository = new CustomerRepository();
        const createCustomerUseCase = new CreateCustomerUseCase(customerRepository);

        const output = await createCustomerUseCase.execute(input);

        expect(output).toStrictEqual({
            id: expect.any(String),
            name: input.name,
            address: {
                street: input.address.street,
                number: input.address.number,
                zip: input.address.zip,
                city: input.address.city,
            },
        });
    });
    it("should throw an error when name is missing", async () => {
        const customerRepository = new CustomerRepository();
        const createCustomerUseCase = new CreateCustomerUseCase(customerRepository);

        input.name = "";

        await expect(createCustomerUseCase.execute(input)).rejects.toThrow("Name is required");
    });

    it("should throw an error when street is missing", async () => {
        const customerRepository = new CustomerRepository();
        const createCustomerUseCase = new CreateCustomerUseCase(customerRepository);

        input.address.street = "";

        await expect(createCustomerUseCase.execute(input)).rejects.toThrow("Street is required");
    });
})