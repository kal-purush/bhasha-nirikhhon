import poupancaAccount from "../../creators/poupancaAccount.model"
import iOpenAccount from "../interfaces/iOpenAccount.model";
import { v4 as uuidv4 } from 'uuid';

export default class openPoupancaAccount implements iOpenAccount{

    create(accountNumber:string,
           amount:number,
           initDate:string):poupancaAccount{
        
        const pa = new poupancaAccount()

        pa._accountID = uuidv4()
        pa._amount = amount 
        pa._isActive = true
        pa._initDate = initDate
        pa._accountNumber = accountNumber

        return pa
    }
}