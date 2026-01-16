import { app, sequelize } from "../express";
import request from "supertest";
import { InputCreateCustomerDto } from "../../../usecase/customer/create/create.customer.dto";

describe("E2E test for customer", () => {
    beforeEach(async () => {
        await sequelize.sync({ force: true });
    });

    afterAll(async () => {
        await sequelize.close();
    });

    it("should create a customer",async () => {
        const response = await request(app)
            .post("/customers")
            .send({
                name: "John",
                address: {
                    street: "Street",
                    city: "City",
                    number: 123,
                    zip: "12345"
                }
            });
        
        expect(response.status).toBe(201);
        expect(response.body.name).toBe("John");
        expect(response.body.address.street).toBe("Street");
        expect(response.body.address.city).toBe("City");
        expect(response.body.address.number).toBe(123);
        expect(response.body.address.zip).toBe("12345");
    });
    it("should not create a customer", async () => {
        const response = await request(app)
            .post("/customers")
            .send({
                name:"John",
            });

        expect(response.status).toBe(500);
    });
    it("should list all customers", async () => {
        const createResponse1 = await createCustomer({
                name: "John",
                address: {
                    street: "Street 1",
                    city: "City 1",
                    number: 1,
                    zip: "11111"
                }
            });
        expect(createResponse1.status).toBe(201);
        const createResponse2 = await createCustomer({
                name: "Jane",
                address: {
                    street: "Street 2",
                    city: "City 2",
                    number: 2,
                    zip: "22222"
                }
            });
        expect(createResponse2.status).toBe(201);

        const findAllResponse = await request(app)
            .get("/customers")
            .send();
        expect(findAllResponse.status).toBe(200);
        expect(findAllResponse.body.customers.length).toBe(2);
        
        const customer1 = findAllResponse.body.customers[0];
        expect(customer1.name).toBe("John");
        expect(customer1.address.street).toBe("Street 1");
        expect(customer1.address.city).toBe("City 1");
        expect(customer1.address.number).toBe(1);
        expect(customer1.address.zip).toBe("11111");
        const customer2 = findAllResponse.body.customers[1];
        expect(customer2.name).toBe("Jane");
        expect(customer2.address.street).toBe("Street 2");
        expect(customer2.address.city).toBe("City 2");
        expect(customer2.address.number).toBe(2);
        expect(customer2.address.zip).toBe("22222");
    });
    it("should find a customer", async () => {
        const createCustomerResponse = await createCustomer({
            name: "John",
            address: {
                street: "Street 1",
                city: "City 1",
                number: 1,
                zip: "11111"
            }
        });
        expect(createCustomerResponse.status).toBe(201);
        const id = createCustomerResponse.body.id;

        const findCustomerResponse = await request(app)
            .get(`/customers/${id}`)
            .send();
        
        expect(findCustomerResponse.status).toBe(200);
        expect(findCustomerResponse.body.id).toBe(id);
        expect(findCustomerResponse.body.name).toBe("John");
        expect(findCustomerResponse.body.address.street).toBe("Street 1");
        expect(findCustomerResponse.body.address.city).toBe("City 1");
        expect(findCustomerResponse.body.address.number).toBe(1);
        expect(findCustomerResponse.body.address.zip).toBe("11111");
    });
    it("should not find a customer", async () => {
        const id = expect.any(String);
        
        const findCustomerResponse = await request(app)
            .get(`/customers/${id}`)
            .send();

        expect(findCustomerResponse.status).toBe(500);
    });
    it("should update a customer",async () => {
        const createCustomerResponse = await createCustomer({
            name: "John",
            address: {
                street: "Street 1",
                city: "City 1",
                number: 1,
                zip: "11111"
            }
        });
        expect(createCustomerResponse.status).toBe(201);

        const id = createCustomerResponse.body.id;
        const updateCustomerDto = {
            id,
            name: "John Updated",
            address: {
                street: "Street 1 Updated",
                city: "City 1 Updated",
                number: 2,
                zip: "22222"
            }
        };

        const updateCustomerResponse = await request(app)
            .put(`/customers/${id}`)
            .send(updateCustomerDto);
        
        expect(updateCustomerResponse.status).toBe(200);
        expect(updateCustomerResponse.body.id).toBe(id);
        expect(updateCustomerResponse.body.name).toBe(updateCustomerDto.name);
        expect(updateCustomerResponse.body.address.street).toBe(updateCustomerDto.address.street);
        expect(updateCustomerResponse.body.address.city).toBe(updateCustomerDto.address.city);
        expect(updateCustomerResponse.body.address.number).toBe(updateCustomerDto.address.number);
        expect(updateCustomerResponse.body.address.zip).toBe(updateCustomerDto.address.zip);
    });
    it("should not update a customer", async () => {
        const id = expect.any(String);
        const updateCustomerDto = {
            id,
            name: "John"
        };

        const updateCustomerResponse = await request(app)
            .put(`/customers/${id}`)
            .send(updateCustomerDto);
        expect(updateCustomerResponse.status).toBe(500);
    });
});

async function createCustomer(createCustomerDto: InputCreateCustomerDto) {
    const response = await request(app)
        .post("/customers")
        .send(createCustomerDto);

    return response;
}