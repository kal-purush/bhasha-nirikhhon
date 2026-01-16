import Client from "../../../creators/client.model";
import { v4 as uuidv4 } from 'uuid';

jest.mock("uuid")
const mockedUuid = jest.mocked(uuidv4)


describe("Client products tests",()=>{
    const c = new Client().createAccount().create("Vitoria", "32354598756")
    c._peopleID = mockedUuid.mockReturnValue("e3235a0c-36f7-4db0-b547-6a18b6bfa098")
    test("Create Client test: should create an Client account", () =>{
        expect(c).toHaveProperty("_isActive",true)
    })
    test("Deactivate Client: should return isActive equal to false", () =>{
        c.deactivateAccount().deactivate(c)

        expect(c).toHaveProperty("_isActive",false)
    })

})