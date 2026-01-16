import iDeposit from "../products/interfaces/iDeposit.model"
import iTransfer from "../products/interfaces/iTransfer.model"
import iWithdrawn from "../products/interfaces/iWithdrawn.model"
import iOpenAccount from "../products/interfaces/iOpenAccount.model"

export default interface iAccount {
    _accountID: string 
    _accountNumber: string 
    _amount: number
    _isActive: boolean
    _initDate: string

     createAccount(isBankManager:boolean): iOpenAccount
     createDeposit(): iDeposit
     createTransfer(): iTransfer
     createWithdrawn(): iWithdrawn 

}