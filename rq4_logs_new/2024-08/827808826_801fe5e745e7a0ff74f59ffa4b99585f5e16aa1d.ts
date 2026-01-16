import currentAccount from "../../Accounts/creators/currentAccount.model";
import iPeople from "../factory/iPeople.model";
import poupancaAccount from "../../Accounts/creators/poupancaAccount.model";
import createClient from "../products/Client/createClient.model";
import deactivateClient from "../products/Client/deactivateClient.model";

export default class Client implements iPeople {
    _peopleID: string 
    _name: string
    _cpf: string
    _isActive: boolean
    _bankManagerID: string
    _currentAccount: currentAccount[]
    _poupancaAccount: poupancaAccount[]

    createAccount():createClient{
        return new createClient()
    }
    deactivateAccount(): deactivateClient{
        return new deactivateClient()
    }

    addCurrentAccount(currentAccount:currentAccount){
        this._currentAccount.push(currentAccount)
    }
    addPoupancaAccount(poupancaAccount:poupancaAccount){
        this._poupancaAccount.push(poupancaAccount)
    }

    

    
}