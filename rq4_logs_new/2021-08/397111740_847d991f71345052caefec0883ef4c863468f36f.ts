import { EHOSTUNREACH } from 'constants';
import Web3 from 'web3';
import { Claim, ClaimHash, CredentialHash, Identity } from './credential';

const _issuerInterfaceJson = require('./contracts/abi/issuer.json');

/**
 * checks if the address is the contract that is an issuer which manages verifiable credentials.
 *
 * @param address ethereum address.
 * @returns true if it's contract or false.
 */
export const isIssuerContract = async (web3: Web3, address: Identity) => {
  return (await web3.eth.getCode(address)) !== '0x';
};

export const defaultClaimParser = (o: Object) => {
  return JSON.stringify(o);
};

export const getIssuer = async (
  web3: Web3,
  issuer: Identity
): Promise<IssuerBase> => {
  if (await isIssuerContract(web3, issuer)) {
    return new ContractIssuer(issuer);
  } else {
    return new WalletIssuer(issuer);
  }
};

export interface IssuerBase {
  issuer: Identity;
  claimParser: (o: Claim) => ClaimHash;
}

export class WalletIssuer implements IssuerBase {
  issuer: Identity;
  claimParser = defaultClaimParser;

  constructor(issuer: Identity) {
    this.issuer = issuer;
  }
}

export class ContractIssuer implements IssuerBase {
  issuer: Identity;
  claimParser = defaultClaimParser;

  constructor(issuer: Identity) {
    this.issuer = issuer;
  }
}