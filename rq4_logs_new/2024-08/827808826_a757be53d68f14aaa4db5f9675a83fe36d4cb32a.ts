import baseAccount from "../baseClasses/baseAccount"
import Client from "../Clients/Client"
import { v4 as uuidv4 } from 'uuid';

export default class CurrentAccount extends baseAccount {
    _client:Client
    _initDate: string
    _isActive: boolean
    _currentAccountID:string
    _currentAccountNumber:number
    _amount: number
    _limitChequeEspecial: number

    constructor(client:Client,
                initDate:string,
                isActive:boolean,
                currentAccountNumber:number,
                limitChequeEspecial:number){
                    super(client,isActive,initDate)
                    this._client = client
                    this._isActive = isActive
                    this._currentAccountID = uuidv4()
                    this._currentAccountNumber = currentAccountNumber
                    this._limitChequeEspecial = limitChequeEspecial
                }

    get amount(){
        return this._amount
    }

    get currentAccountNumber(){
        return this._currentAccountNumber
    }
    get isActive() {
        return this._isActive
    }
    get limitChequeEspecial(){
        return this._currentAccountNumber
    }
    set LimitChequeEspecial(new_limit:number){
        this._limitChequeEspecial = new_limit
    }

    set isActive(newActive:boolean){
         this._isActive = newActive
    }

    set amount(new_amount:number){
        this._amount = new_amount
    }
}