import {
  generateKeyPair as gKeyPair,
  getKeyPairFromMnemonic as gMemPair,
} from 'human-crypto-keys';
import { IHumanCryptoKey } from './interfaces/IHumanCryptoKey.interface';

export const ALGORITHM = 'ed25519';

/**
 * Generates a key pair with rsa encryption and provides information for recovery.
 * @returns {Promise<IHumanCryptoKey>} containing the public and private key, as well as the algorithm  and mnemonic used
 */
export const generateKeyPair = async (): Promise<IHumanCryptoKey> => {
  return await gKeyPair(ALGORITHM);
};

/**
 * Generates the same key pair based on the mnemonic.
 * @param mnemonic mnemonic phrase
 * @returns {Promise<IHumanCryptoKey>} contains the private key and public key
 */
export const generateKeyPairFromMnemonic = async (
  mnemonic: string
): Promise<IHumanCryptoKey> => {
  return await gMemPair(mnemonic, ALGORITHM);
};