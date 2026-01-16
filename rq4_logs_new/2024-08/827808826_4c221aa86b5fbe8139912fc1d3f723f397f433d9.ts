import bankManager from "../../../creators/bankManager.model";
import { v4 as uuidv4 } from 'uuid';

jest.mock("uuid")
const mockedUuid = jest.mocked(uuidv4)


describe("bankManager products tests",()=>{
    const bm = new bankManager().createAccount().create("Marisa", "23287498734")
    bm._peopleID = mockedUuid.mockReturnValue("e2519a0c-36f7-4db0-b380-6a18b6bfa790")
    bm._bankManagerToken = mockedUuid.mockReturnValue()
    test("Create BankManager test: should create an bankManager account", () =>{
        expect(bm).toHaveProperty("_isActive",true)
    })
    test("Deactivate BankManager: should return isActive equal to false", () =>{
        bm.deactivateAccount().deactivate(bm)

        expect(bm).toHaveProperty("_isActive",false)
    })

})