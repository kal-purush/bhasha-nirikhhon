import { Sequelize } from "sequelize-typescript";
import Customer from "../../../domain/customer/entity/customer";
import CustomerRepository from "../../../domain/customer/repository/customer.repository";
import Address from "../../../domain/customer/value-objects/address";
import CustomerModel from "../../../infrastructure/customer/repository/sequelize/customer.model";
import FindCustomerUseCase from "./find.customer.usecase";

describe('Test find customer use case', () => {
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

    it("should find a customer", async () => {
        const customerRepository = new CustomerRepository();
        const usecase = new FindCustomerUseCase(customerRepository);

        const customer = new Customer("123", "John");
        const address = new Address("Street", 123, "zip", "City");
        customer.changeAddress(address);

        await customerRepository.create(customer);

        const input = {
            id: "123",
        }

        const output = {
            id: "123",
            name: "John",
            address: {
                street: "Street",
                city: "City",
                zip: "zip",
                number: 123
            }
        }

        const result = await usecase.execute(input);

        expect(result).toStrictEqual(output);
    });
});