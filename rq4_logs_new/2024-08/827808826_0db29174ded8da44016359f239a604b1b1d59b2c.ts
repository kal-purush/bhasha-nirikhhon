import currentAccount from "./creators/currentAccount.model";
import poupancaAccount from "./creators/poupancaAccount.model";
export declare class AccountsService {
    private readonly filePathCC;
    private readonly filePathPA;
    private readCurrentAccounts;
    private writeCurrentAccounts;
    private readPoupancaAccounts;
    private writePoupancaAccounts;
    createCurrentAccounts(isBankManager: boolean, accountNumber: string, amount: number, initDate: string): currentAccount;
    findCurrentAccountById(id: string): currentAccount;
    updateCurrentAccount(id: string, amount: number, initDate: string, accountNumber: string): currentAccount;
    deactivateCurrentAccountById(id: string): currentAccount;
    createPoupancaAccount(isBankManager: boolean, accountNumber: string, amount: number, initDate: string): poupancaAccount;
    findPoupancaAccountById(id: string): poupancaAccount;
    updatePoupancaAccount(id: string, amount: number, initDate: string, accountNumber: string): poupancaAccount;
    deactivatePoupancaAccountById(id: string): poupancaAccount;
}