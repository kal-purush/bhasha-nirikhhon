import Web3 from 'web3';
import { Contract } from 'web3-eth-contract';
import { RelayingServicesAddresses } from './interfaces';
export declare class Contracts {
    private readonly web3Instance;
    readonly addresses: RelayingServicesAddresses;
    private smartWalletFactory;
    private smartWalletRelayVerifier;
    private smartWalletDeployVerifier;
    constructor(web3Instance: Web3, chainId: string);
    initialize(): Promise<void>;
    getSmartWalletFactory(): Contract;
    getSmartWalletRelayVerifier(): Contract;
    getSmartWalletDeployVerifier(): Contract;
}