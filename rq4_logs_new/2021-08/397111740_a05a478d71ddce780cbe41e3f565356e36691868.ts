import Web3 from 'web3';
import { Contract } from 'web3-eth-contract';
import { Claim, ClaimHash, Identity } from './credential';

export interface IDIDManager {
  web3: Web3;
  registry: Contract;

  issuer: (issuer: Identity) => Promise<IssuerBase>;

  getRegistryDomainSeparator(): Promise<string>;
}

export interface IssuerBase {
  issuer: Identity;
  claimParser: (o: Claim) => ClaimHash;

  issueVerfiaibleCredential(identity: Identity, claim: Claim): Promise<void>;
  getCredentialHash(identity: Identity, claim: Claim): Promise<string>;
}