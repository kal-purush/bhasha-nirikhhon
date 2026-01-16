import iPeople from "../factory/iPeople.model";
import { v4 as uuidv4 } from 'uuid';
import deactivateClientAccount from "../products/ClientAccount/deactivateClientAccount.model";
import clientAccount from "src/clientAccount/clientAccount.model";

export default class bankManager implements iPeople {
    _peopleID: string 
    _name: string
    _cpf: string
    _isActive: boolean
    _bankManagerToken: string

    constructor(
        name:string,
        cpf:string,
        isActive:boolean
    ){
        this._peopleID = uuidv4()
        this._name = name 
        this._cpf = cpf
        this._isActive = isActive
        this._bankManagerToken = uuidv4()
    }

    private validateOperation(token:string){
        if (token !== this._bankManagerToken){
            return false
        }
        return true
    }
}