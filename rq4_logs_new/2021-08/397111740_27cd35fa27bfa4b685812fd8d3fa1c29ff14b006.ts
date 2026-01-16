import { expect } from 'chai';
import chai from 'chai';
import Web3 from 'web3';
import { Claim, Identity } from '../src/interfaces/credential';
import { DID } from './../src/did';
import {
  provider,
  registry as _registry,
  issuerDomain as _issuerDomain,
} from './environment';
import { Issuer } from '../src/interfaces';

chai.use(require('chai-as-promised'));

describe('ContractIssuer', () => {
  const domainName = `Contract Domain ${Math.floor(Math.random() * 10000000)}`;
  const issuerAbi = require('../src/contracts/abi/issuer.interface.json');
  const web3 = new Web3(provider);
  const did = new DID({
    web3,
    registry: _registry,
    issuerDomain: _issuerDomain,
  });

  let issuerAddress: string;
  let identities: Identity[];
  let issuer: Issuer;

  const claim: Claim = {
    alumniOf: {
      name: 'Hanyang Univ',
    },
  };

  const expirationGenerator = (seconds?: number) =>
    Math.floor(new Date().getTime() / 1000) + (seconds ? seconds : 86400);

  before(async () => {
    [issuerAddress, ...identities] = await web3.eth.getAccounts();

    const contractAddress = (await did.issuer.create(domainName, '', {
      from: issuerAddress,
    })) as any;

    issuer = await did.issuer.domain(domainName);
  });

  it('Should credential hash by contract call', async () => {
    expect(issuer.getCredentialHash(identities[0], claim)).to.not.rejected;
  });

  it('Should delegate other', async () => {
    expect(issuer.delegate(identities[0], expirationGenerator())).to.not
      .rejected;
  });

  it('Should generate credential by owner', async () => {
    expect(
      issuer.issueVerfiaibleCredential(
        identities[1],
        claim,
        expirationGenerator(),
        { from: issuerAddress }
      )
    ).to.be.not.rejected;
  });

  it('Should generate credential by delegate', async () => {
    expect(
      issuer.issueVerfiaibleCredential(
        identities[2],
        claim,
        expirationGenerator(),
        { from: identities[0] }
      )
    ).to.be.not.rejected;
  });

  it('Should not generate credential by others', async () => {
    expect(
      issuer.issueVerfiaibleCredential(
        identities[3],
        claim,
        expirationGenerator(),
        { from: identities[3] }
      )
    ).to.be.rejected;
  });
});