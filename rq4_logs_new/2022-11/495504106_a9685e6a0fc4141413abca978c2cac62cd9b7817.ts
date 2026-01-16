import BN from 'bn.js';
import { ethers } from 'ethers'
import elliptic from 'elliptic'

import {
    StarkwareRegistration,
    NetworkId,
    KeyPair,
} from '../types';
import {
    stripHexPrefix,
    bnToHex32,
    hexToBn
} from '../lib/util';
import {
    sign,
} from '../lib/crypto';
import {
    asEcKeyPair,
} from '../helpers';
import { StarkSignable } from './stark-signable';

const REGISTRATION_PREFIX = "UserRegistration:"
const K_MODULUS = new BN('3618502788666131213697322783095070105526743751716087489154079457884512865583', 10)

/**
 * Wrapper object to convert a transfer, and hash, sign, and verify its signature.
 */
export class SignableRegistration extends StarkSignable<StarkwareRegistration> {

    static fromRegistration(
        ethKey: string,
        starkKey: string,
        networkId: NetworkId,
    ): SignableRegistration {
        return new SignableRegistration(
            {
                ethKey,
                starkKey
            },
            networkId
        );
    }

    protected async calculateHash(): Promise<BN> {
        const ethKey = Buffer.from(stripHexPrefix(this.message.ethKey), 'hex');
        const starkKey = Buffer.from(stripHexPrefix(this.message.starkKey), 'hex');
        const hash = (await ethers.utils.solidityKeccak256(['string', 'address', 'uint256'], [REGISTRATION_PREFIX, ethKey, starkKey]))
        const hashBn = new BN(stripHexPrefix(hash), 'hex')
        return hashBn.mod(K_MODULUS)
    }


    /**
     * Sign the message with the given private key and employs custom sig packing to conform to
     * the smart contract function
     */
    async sign(
        keys: KeyPair,
    ): Promise<string> {
        const hashBN = await this.getHashBN();
        const ecSignature = await sign(asEcKeyPair(keys.privateKey), hashBN);
        return this.serializeSignature((ecSignature), keys.publicKeyYCoordinate!);
    }

    serializeSignature(sig: elliptic.ec.Signature, starkY: string): string {
        const y = hexToBn(starkY)
        return `0x${sig.r.iushln(32 * 8).iadd(sig.s).iushln(32 * 8).iadd(y).toString('hex', 96)}`
    }


    toStarkware(): StarkwareRegistration {
        return this.message;
    }
}