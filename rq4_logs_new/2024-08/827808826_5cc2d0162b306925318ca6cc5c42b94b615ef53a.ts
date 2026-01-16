import { UnauthorizedException } from "@nestjs/common";
import openCurrentAccount from "../../products/currentAccount/openCurrentAccount.model";
import currentAccount from "../currentAccount.model";
import depositCurrentAccount from "../../products/currentAccount/depositCurrentAccount.model";
import transferCurrentAccount from "../../products/currentAccount/transferCurrentAccount.model";
import withdrawnCurrentaAccount from "../../products/currentAccount/withdrawnCurrentAccount.model";


describe("currentAccount products tests",()=>{
    const account: currentAccount  = new currentAccount()
    test("should return an OpenAccount object", () =>{
        expect(account.createOpenAccount(true)).toBeInstanceOf(openCurrentAccount)
    })
    test("should throw an Unauthorized operation if bankManager is false", () => {
        expect(()=>account.createOpenAccount(false)).toThrow(UnauthorizedException)
    })
    test("create new current account desposit", () => {
        expect(account.createDeposit()).toBeInstanceOf(depositCurrentAccount)
    })
    test("create new current account transfer", () => {
        expect(account.createTransfer()).toBeInstanceOf(transferCurrentAccount)
    })
    test("create new current account transfer", () => {
        expect(account.createWithdrawn()).toBeInstanceOf(withdrawnCurrentaAccount)
    })
})