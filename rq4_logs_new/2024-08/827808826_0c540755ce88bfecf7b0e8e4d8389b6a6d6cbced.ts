import currentAccount from "../../../creators/currentAccount.model";
import depositCurrentAccount from "../depositCurrentAccount.model"
import { v4 as uuidv4 } from 'uuid';
import withdrawnCurrentAccount from "../withdrawnCurrentAccount.model";
import { UnauthorizedException } from "@nestjs/common";
import transferCurrentAccount from "../transferCurrentAccount.model";

jest.mock("uuid")
const mockedUuid = jest.mocked(uuidv4)


describe("currentAccount products tests",()=>{
    test("Deposit test: should increase the value of currentAccount with an amount", () =>{
        const cc = new currentAccount().createOpenAccount(true).create(
            "4324",150,"2023-5-17"
        )
        cc._accountID = mockedUuid.mockReturnValue("132871ac-a76a-40fb-874a-17c42cc76d8c")
        const newDeposit = new depositCurrentAccount()
        newDeposit.processDeposit(50,cc)

        expect(cc._amount).toBe(200)
    })
    test("Deposit test: should increase the value of currentAccount without an amount", () =>{
        const cc = new currentAccount().createOpenAccount(true).create(
            "4212",0,"2023-5-23"
        )
        cc._accountID = mockedUuid.mockReturnValue("132871ac-a76a-40fb-874a-17c41cc76d8c")
        const newDeposit = new depositCurrentAccount()
        newDeposit.processDeposit(50,cc)

        expect(cc._amount).toBe(50)
    })
    test("Withdrawn test: should decrease the value of currentAccount with an amount", () =>{
        const cc = new currentAccount().createOpenAccount(true).create(
            "2372",40,"2022-5-21"
        )
        cc._accountID = mockedUuid.mockReturnValue("137871ac-a76a-40fb-124a-17c41cc76d8c")
        const newWithdrawn = new withdrawnCurrentAccount()
        newWithdrawn.processWithdrawn(20,cc)

        expect(cc._amount).toBe(20)
    })
    test("Withdrawn test: should not make an withdrawn with currentAccount without amount", () =>{
        const cc = new currentAccount().createOpenAccount(true).create(
            "1254",0,"2022-3-12"
        )
        cc._accountID = mockedUuid.mockReturnValue("542871ac-a76a-32fb-874a-17c41cc76d8c")
        const newWithdrawn = new withdrawnCurrentAccount()

        expect(()=>newWithdrawn.processWithdrawn(20,cc)).toThrow(UnauthorizedException)
    })
    test("Transfer test: should make a transfer with a current account with an amount to another current amount", () =>{
        const ccMakeTransfer = new currentAccount().createOpenAccount(true).create(
            "3214",100,"2022-3-12"
        )
        const ccRecieveTransfer = new currentAccount().createOpenAccount(true).create(
            "2321",300,"2012-12-12"
        )
        ccMakeTransfer._accountID = mockedUuid.mockReturnValue("542871ac-a76a-32fb-874a-17c41cc76d8c")
        ccRecieveTransfer._accountID = mockedUuid.mockReturnValue("542321ac-a76a-32fb-874a-12c41cc76d8c")
        const newTransfer = new transferCurrentAccount()
        newTransfer.processTransfer(20,ccMakeTransfer,ccRecieveTransfer)

        expect(ccRecieveTransfer).toHaveProperty("_amount",320)
        expect(ccMakeTransfer).toHaveProperty("_amount",80)
        
    })
    test("Transfer test: should not make a transfer if the amount to transfer is bigger then the current amount from the current account making the transfer", () =>{
        const ccMakeTransfer = new currentAccount().createOpenAccount(true).create(
            "3214",100,"2022-3-12"
        )
        const ccRecieveTransfer = new currentAccount().createOpenAccount(true).create(
            "2321",300,"2012-12-12"
        )
        ccMakeTransfer._accountID = mockedUuid.mockReturnValue("542871ac-a76a-32fb-874a-17c41cc76d8c")
        ccRecieveTransfer._accountID = mockedUuid.mockReturnValue("542321ac-a76a-32fb-874a-12c41cc76d8c")
        const newTransfer = new transferCurrentAccount()

        expect(()=>newTransfer.processTransfer(200,ccMakeTransfer,ccRecieveTransfer)).toThrow(UnauthorizedException)
        
    })
    test("Create Account Test: a new current account should not have a limitChequeEspecial defined",()=>{
        const cc = new currentAccount().createOpenAccount(true).create(
            "1234",200,"2021-3-22"
        )

        expect(cc).not.toHaveProperty("_limitChequeEspecial")
    }
    )
    test("Deactivate Account: a current account when deactivate show have _isActive equal to false",()=>{
        const cc = new currentAccount().createOpenAccount(true).create(
            "1234",200,"2021-3-22"
        )
        const newDeactivate = cc.startDeactivate()
        newDeactivate.deactivate(cc)
        expect(cc).toHaveProperty("_isActive",false)
    }
    )
})