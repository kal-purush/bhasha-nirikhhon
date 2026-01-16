import { UnauthorizedException } from "@nestjs/common";
import poupancaAccount from "../poupancaAccount.model";
import openPoupancaAccount from "../../products/poupancaAccount/openPoupancaAccount.model";
import depositPoupancaAccount from "../../products/poupancaAccount/depositPoupancaAccount.model";
import transferPoupancaAccount from "../../products/poupancaAccount/transferPoupancaAccount.model";
import withdrawnPoupancaAccount from "../../products/poupancaAccount/withdrawnPoupancaAccount.model";


describe("PoupancaAccount products tests",()=>{
    const account: poupancaAccount  = new poupancaAccount()
    test("should return an OpenAccount object", () =>{
        expect(account.createOpenAccount(true)).toBeInstanceOf(openPoupancaAccount)
    })
    test("should throw an Unauthorized operation if bankManager is false", () => {
        expect(()=>account.createOpenAccount(false)).toThrow(UnauthorizedException)
    })
    test("create new Poupanca account desposit", () => {
        expect(account.createDeposit()).toBeInstanceOf(depositPoupancaAccount)
    })
    test("create new Poupanca account transfer", () => {
        expect(account.createTransfer()).toBeInstanceOf(transferPoupancaAccount)
    })
    test("create new Poupanca account transfer", () => {
        expect(account.createWithdrawn()).toBeInstanceOf(withdrawnPoupancaAccount)
    })
})