// import { expect } from 'chai';
import chai from 'chai';
import Web3 from 'web3';
import { Issuer } from '../src/interfaces';
import { Claim } from '../src/interfaces/credential';
import { DID } from './../src/did';
import { provider, registry, issuerDomain } from './environment';

chai.use(require('chai-as-promised'));
const expect = chai.expect;

describe('Identity', async () => {
  const web3 = new Web3(provider);
  const did = new DID({ web3, registry, issuerDomain });

  let issuerAddress: string;
  let issuerContractAddr: string;
  let issuer: Issuer;
  let identities: string[];

  const claim: Claim = {
    alumniOf: {
      name: 'Hanyang Univ',
    },
  };

  const wrongClaim: Claim = {
    alumniOf: {
      name: 'Seoul Univ',
    },
  };

  const expirationGenerator = (seconds?: number) =>
    Math.floor(new Date().getTime() / 1000) + (seconds ? seconds : 86400);

  before(async () => {
    [issuerAddress, ...identities] = await web3.eth.getAccounts();
    issuerContractAddr = await did.issuer.create(
      `Identity Test ${Math.floor(Math.random() * 10000000)}`,
      '',
      { from: issuerAddress }
    );
    issuer = await did.issuer.at(issuerContractAddr);
  });

  it('Should generate credential for identity', async () => {
    await expect(
      issuer.issueVerfiaibleCredential(
        identities[0],
        claim,
        expirationGenerator(),
        { from: issuerAddress }
      )
    ).to.be.fulfilled;
  });

  it('Should validate credential for identity', async () => {
    const identity = await did.identity.at(identities[0]);
    await expect(
      identity.validate(issuer.issuer, claim)
    ).eventually.to.be.equal(true);
  });

  it('Should fail for invalid credential', async () => {
    const identity = await did.identity.at(identities[0]);
    await expect(
      identity.validate(issuer.issuer, wrongClaim)
    ).eventually.to.be.equal(false);
  });

  it('Should fail to other identity for the same claim', async () => {
    const identity = await did.identity.at(identities[1]);
    await expect(
      identity.validate(issuer.issuer, claim)
    ).eventually.to.be.equal(false);
  });
});