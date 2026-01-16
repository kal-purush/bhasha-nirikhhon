import currentAccount from '../../domain/currentAccount.model';
import { CurrentAccountsService } from '../../application/current-accounts.service';
export declare class currentAccountController {
    private readonly currentAccountService;
    constructor(currentAccountService: CurrentAccountsService);
    createCurrentAccount(isBankManager: boolean, amount: number, initDate: string, accountNumber: string): Promise<currentAccount>;
    findCurrentAccountByID(id: string): Promise<currentAccount>;
    updateCurrentAccountById(id: string, initDate: string, amount: number, accountNumber: string): Promise<currentAccount>;
    deactivateClientByID(id: string): Promise<currentAccount>;
}