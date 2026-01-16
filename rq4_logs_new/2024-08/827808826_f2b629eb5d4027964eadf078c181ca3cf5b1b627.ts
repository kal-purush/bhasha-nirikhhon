import currentAccount from "../../creators/currentAccount.model"
import iOpenAccount from "../interfaces/iOpenAccount.model";
import { v4 as uuidv4 } from 'uuid';

export default class openCurrentAccount implements iOpenAccount{

    create(accountNumber:string,
           amount:number,
           initDate:string):currentAccount{
        
        const cc = new currentAccount()

        cc._accountID = uuidv4()
        cc._amount = amount 
        cc._isActive = true
        cc._initDate = initDate
        cc._accountNumber = accountNumber

        return cc
    }
}