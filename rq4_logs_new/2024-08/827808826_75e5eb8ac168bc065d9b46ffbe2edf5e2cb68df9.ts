import { UnauthorizedException } from '@nestjs/common';
import { typeAccount } from '../../enum/account.enum';
import currentAccount from '../creators/currentAccount.model';
import poupancaAccount from '../creators/poupancaAccount.model';

export default class accountFactory {
    private validateOperation(isBankManager:boolean){
        if(!isBankManager){
            return false
        }
        return true
    }

    createAccount(isBankManager:boolean,
                  type:typeAccount,
                  amount:number,
                  initDate:string){
        if(!this.validateOperation(isBankManager)){
            throw new UnauthorizedException(`Invalid operation`)
        }
        
        switch(type){
            case typeAccount.CURRENT:
                const ccAccount = new currentAccount(amount,initDate)
                return ccAccount
            case typeAccount.POUPANCA:
                const pAccount = new poupancaAccount(amount,initDate)
                return pAccount
        }
    }

}