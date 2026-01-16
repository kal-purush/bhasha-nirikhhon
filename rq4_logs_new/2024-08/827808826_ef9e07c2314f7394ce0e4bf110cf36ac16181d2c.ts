import iAccount from "../factories/iAccount.model";
import { v4 as uuidv4 } from 'uuid';
import depositCurrentAccount from "../products/currentAccount/depositCurrentAccount.model"
import transferCurrentAccount from "../products/currentAccount/transferCurrentAccount.model";
import withdrawnCurrentAccount from "../products/currentAccount/withdrawnCurrentAccount.model";
import openCurrentAccount from "../products/currentAccount/openCurrentAccount.model";
import { ExceptionsHandler } from "@nestjs/core/exceptions/exceptions-handler";

export default class currentAccount implements iAccount {
    _accountID: string 
    _accountNumber: string 
    _amount: number
    _isActive: boolean
    _initDate: string
    _limitChequeEspecial:number
    
    private validadeOperation(isBankManager:boolean){
        if(!isBankManager){
            return false
        }
        return true
    }

    createAccount(isBankManager:boolean):openCurrentAccount{
        if(!this.validadeOperation(isBankManager)){
            throw new ExceptionsHandler()
        }
        return  new openCurrentAccount()
    }

    createDeposit(): depositCurrentAccount {
        return new depositCurrentAccount()
    }
    createTransfer(): transferCurrentAccount {
        return new transferCurrentAccount()
    }
    createWithdrawn(): withdrawnCurrentAccount {
        return new withdrawnCurrentAccount()
    }
}
