import poupancaAccount from "../../../creators/poupancaAccount.model";
import depositPoupancaAccount from "../depositPoupancaAccount.model";
import { v4 as uuidv4 } from 'uuid';
import withdrawnPoupancaAccount from "../withdrawnPoupancaAccount.model";
import { UnauthorizedException } from "@nestjs/common";
import transferPoupancaAccount from "../transferPoupancaAccount.model";
jest.mock("uuid")
const mockedUuid = jest.mocked(uuidv4)


describe("poupancaAccount products tests",()=>{
    test("Deposit test: should increase the value of poupanca account with an amount", () =>{
        const cc = new poupancaAccount().createOpenAccount(true).create(
            "4324",150,"2023-5-17"
        )
        cc._accountID = mockedUuid.mockReturnValue("9a469d85-5f56-4637-b10f-4bb0dcdc2713")
        const newDeposit = new depositPoupancaAccount()
        newDeposit.processDeposit(50,cc)

        expect(cc._amount).toBe(200)
    })
    test("Deposit test: should increase the value of poupanca account without an amount", () =>{
        const cc = new poupancaAccount().createOpenAccount(true).create(
            "4212",0,"2023-5-23"
        )
        cc._accountID = mockedUuid.mockReturnValue("9a469d85-5f43-4637-b10f-4bb0dcdc9876")
        const newDeposit = new depositPoupancaAccount()
        newDeposit.processDeposit(50,cc)

        expect(cc._amount).toBe(50)
    })
    test("Withdrawn test: should decrease the value of poupanca account with an amount", () =>{
        const cc = new poupancaAccount().createOpenAccount(true).create(
            "2372",40,"2022-5-21"
        )
        cc._accountID = mockedUuid.mockReturnValue("9a469d85-5f56-7543-b10f-4bb0dcdc2713")
        const newWithdrawn = new withdrawnPoupancaAccount()
        newWithdrawn.processWithdrawn(20,cc)

        expect(cc._amount).toBe(20)
    })
    test("Withdrawn test: should not make an withdrawn with poupanca account without amount", () =>{
        const cc = new poupancaAccount().createOpenAccount(true).create(
            "1254",0,"2022-3-12"
        )
        cc._accountID = mockedUuid.mockReturnValue("813460a5-7d42-46cf-8a70-495dc608b8f2")
        const newWithdrawn = new withdrawnPoupancaAccount()

        expect(()=>newWithdrawn.processWithdrawn(20,cc)).toThrow(UnauthorizedException)
    })
    test("Transfer test: should make a transfer with a poupanca account with an amount to another poupanca amount", () =>{
        const ccMakeTransfer = new poupancaAccount().createOpenAccount(true).create(
            "3214",100,"2022-3-12"
        )
        const ccRecieveTransfer = new poupancaAccount().createOpenAccount(true).create(
            "2321",300,"2012-12-12"
        )
        ccMakeTransfer._accountID = mockedUuid.mockReturnValue("813460a5-7d42-46cf-8a70-495dc608b8f2")
        ccRecieveTransfer._accountID = mockedUuid.mockReturnValue("987260a5-7d42-46cf-8a70-527dc608b8f2")
        const newTransfer = new transferPoupancaAccount()
        newTransfer.processTransfer(20,ccMakeTransfer,ccRecieveTransfer)

        expect(ccRecieveTransfer).toHaveProperty("_amount",320)
        expect(ccMakeTransfer).toHaveProperty("_amount",80)
        
    })
    test("Transfer test: should not make a transfer if the amount to transfer is bigger then the poupanca amount from the poupanca account making the transfer", () =>{
        const ccMakeTransfer = new poupancaAccount().createOpenAccount(true).create(
            "3214",100,"2022-3-12"
        )
        const ccRecieveTransfer = new poupancaAccount().createOpenAccount(true).create(
            "2321",300,"2012-12-12"
        )
        ccMakeTransfer._accountID = mockedUuid.mockReturnValue("b2c7e517-ffaf-4643-9efd-7214fe210a66")
        ccRecieveTransfer._accountID = mockedUuid.mockReturnValue("g2c7e234-ffaf-4643-9efd-1256fe210a66")
        const newTransfer = new transferPoupancaAccount()

        expect(()=>newTransfer.processTransfer(200,ccMakeTransfer,ccRecieveTransfer)).toThrow(UnauthorizedException)
        
    })
    test("Create poupanca account Test: a new poupanca account should not have a rendimento defined",()=>{
        const cc = new poupancaAccount().createOpenAccount(true).create(
            "1234",200,"2021-3-22"
        )

        expect(cc).not.toHaveProperty("_rendimento")
    }
    )
    test("Deactivate account: a poupanca account when deactivate show have _isActive equal to false",()=>{
        const cc = new poupancaAccount().createOpenAccount(true).create(
            "1234",200,"2021-3-22"
        )
        const newDeactivate = cc.startDeactivate()
        newDeactivate.deactivate(cc)
        expect(cc).toHaveProperty("_isActive",false)
    }
    )
})