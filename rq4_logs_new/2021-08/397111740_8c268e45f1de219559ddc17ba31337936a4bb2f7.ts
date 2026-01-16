import {
  DomainConfig,
  DomainQueryConfig,
  IDIDController,
  IDIDManager,
  IIssuerManager,
  Issuer,
} from './interfaces';
import { Identity } from './interfaces/credential';
import { getIssuer } from './issuer';
import { SendOptions, Contract } from 'web3-eth-contract';
import { keccak256 } from './utils';

const _issuerDomainInterfaceJson = require('./contracts/abi/issuer-domain.interface.json');

export class IssuerManager implements IIssuerManager, IDIDController {
  did: IDIDManager;

  constructor(did: IDIDManager) {
    this.did = did;
  }

  async at(issuer: Identity): Promise<Issuer> {
    return await getIssuer(this.did, issuer);
  }

  async domain(
    domainName: string,
    config?: DomainQueryConfig
  ): Promise<Issuer> {
    const { web3 } = this.did;

    // get issuer domain contract
    let issuerDomain: Contract;

    if (config?.issuerDomain) {
      issuerDomain = new web3.eth.Contract(
        _issuerDomainInterfaceJson,
        config?.issuerDomain
      );
    } else {
      issuerDomain = this.did.issuerDomain as Contract;
    }

    // calculate domain hash
    const domainHash = web3.utils.keccak256(
      web3.eth.abi.encodeParameter('string', domainName)
    );

    // query issuer address by domain name
    const address = await issuerDomain?.methods.getDomain(domainHash).call();

    if (address === '0x0000000000000000000000000000000000000000') {
      throw new Error('Issuer not found for the domain');
    }

    return this.at(address);
  }

  async create(
    domainName: string,
    icon: string,
    txOptions: SendOptions
  ): Promise<Identity> {
    const { issuerDomain } = this.did;
    let extendedParams: any[] = [];

    if (!issuerDomain) {
      throw new Error('Issuer domain should be defined to generate issuer');
    }

    if (icon) {
      extendedParams = [...extendedParams, icon];
    }

    const { events } = await issuerDomain.methods
      .createDomain(domainName, ...extendedParams)
      .send(txOptions || {});

    return this.did.web3.eth.abi.decodeParameter(
      'address',
      events[0].raw.topics[1]
    ) as any as string;
  }
}