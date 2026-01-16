import { UnauthorizedException } from "@nestjs/common";
import bankManager from "../bankManager.model";
import createBankManager from "../../products/BankManager/createBankManager.model";
import deactivateBankManager from "../../products/BankManager/deactivateBankManager.model";



describe("bankManager create products tests",()=>{
    const account: bankManager  = new bankManager()
    test("should return an createBankManager object", () =>{
        expect(account.createAccount()).toBeInstanceOf(createBankManager)
    })
    test("should return an deactivateBankManager object", () => {
        expect(account.deactivateAccount()).toBeInstanceOf(deactivateBankManager)
    })
   
})