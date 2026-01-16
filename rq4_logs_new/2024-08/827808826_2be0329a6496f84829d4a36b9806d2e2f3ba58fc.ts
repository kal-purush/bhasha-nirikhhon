import iCreatePeopleType from "../interfaces/iCreatePeopleType.model";
import bankManager from "../../creators/bankManager.model";
import { v4 as uuidv4 } from 'uuid';

export default class createBankManager implements iCreatePeopleType {
    create(name: string, cpf: string){
        const account = new bankManager()
        account._name = name 
        account._cpf = cpf 
        account._isActive = true
        account._peopleID = uuidv4()
        account._bankManagerToken = uuidv4()
        return account
    }
}