import BN = require('bn.js');
import { ContractWrapper } from './contract-wrapper';
export declare class FanToken extends ContractWrapper {
    constructor(web3: any, networkId: number);
    balanceOf(user: string): Promise<BN>;
    approve(spender: string, amount: BN, from: string): Promise<any>;
    increaseAllowance(spender: string, amount: BN, from: string): Promise<any>;
    decreaseAllowance(spender: string, amount: BN, from: string): Promise<any>;
    getContractAddress(): any;
    private _getFanTokenInstance;
}