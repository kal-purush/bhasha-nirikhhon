import { Claim, Identity } from './interfaces/credential';
import { IDIDManager } from './interfaces/did';

// parse DID identity into 3 parts. (https://www.w3.org/TR/did-core/#a-simple-example)
// ex) did:ethr:0xabc => { did, ethr, 0xabc }
export const parseIdentity = (identity: Identity) => {
  let scheme, method, identifier;

  const parts = identity.split(':');

  if (parts.length === 1) {
    // if no colon found, use default scheme and method.
    scheme = 'did';
    method = 'ethr';
    identifier = parts[0];
  } else if (parts.length === 3) {
    // it's real identity syntax.
    [scheme, method, identifier] = parts;
  } else {
    // other cannot be parsed.
    throw new Error('Unknown identity type');
  }

  return { scheme, method, identifier };
};

export class IdentityController {
  did: IDIDManager;
  identity: Identity;
  _addr: string;

  constructor(did: IDIDManager, identity: Identity) {
    this.did = did;
    const parts = parseIdentity(identity);
    this.identity = `${parts.scheme}:${parts.method}:${parts.identifier}`;
    this._addr = parts.identifier;
  }

  async validate(issuer: Identity, claim: Claim): Promise<boolean> {
    const _issuer = await this.did.issuer.at(issuer);
    return await _issuer.validateCredential(this._addr, claim);
  }
}