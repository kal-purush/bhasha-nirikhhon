import { Keypair } from '@solana/web3.js';
import fs from 'fs';
import path from 'path';

// define default locations
const DEFAULT_KEY_DIR_NAME = '.local_keys';

/**
 * Load a locally stored JSON keypair file and convert it to
 * a valid Keypair
 */

export function loadKeypairFromFile(absPath) {
  try {
    if (!absPath) throw new Error('No path provided');
    if (!fs.existsSync(absPath)) throw new Error('File not found');

    const data = fs.readFileSync(absPath, { encoding: 'utf-8' });
    const keypair = Keypair.fromSecretKey(
      Buffer.from(JSON.parse(data))
    );
    return keypair;
  } catch (error) {
    throw error;
  }
}

/**
 * Save a locally stored JSON keypair file for later importing
 */

export function saveKeypairToFile(
  keypair: Keypair,
  fileName: string,
  dirName: string = DEFAULT_KEY_DIR_NAME
) {
  try {
    fileName = path.join(dirName, `${fileName}.json`);

    // create the 'dirName' directory if it doesn't exist
    if (!fs.existsSync(`./${dirName}/`))
      fs.mkdirSync(`./${dirName}/`);

    // remove the current file if it exists
    if (fs.existsSync(fileName)) fs.unlinkSync(fileName);

    // write the `secretKey` value as a string
    fs.writeFileSync(fileName, `[${keypair.secretKey.toString()}]`, {
      encoding: 'utf-8',
    });

    return fileName;
  } catch (error) {
    throw error;
  }
}

/**
 * Load a keypair from the filesystem, or generate a new one and store it
 */

export function loadOrGenerateKeypair(
  fileName: string,
  dirName: string = DEFAULT_KEY_DIR_NAME
) {
  try {
    const searchPath = path.join(dirName, `${fileName}.json`);
    let keypair = Keypair.generate();

    if (fs.existsSync(searchPath))
      keypair = loadKeypairFromFile(searchPath);
    else saveKeypairToFile(keypair, fileName, dirName);

    return keypair;
  } catch (error) {
    console.log('loadOrGenerateKeypair error', error);
    throw error;
  }
}

export function exploreURL({
  address,
  txSignature,
  cluster,
}: {
  address?: string;
  txSignature?: string;
  cluster?: 'devnet' | 'testnet' | 'mainnet-beta' | 'mainnet';
}) {
  let baseUrl: string;

  if (address)
    baseUrl = `https://explorer.solana.com/address/${address}`;
  else if (txSignature)
    baseUrl = `https://explorer.solana.com/tx/${txSignature}`;
  else return '[unknown]';

  // auto append the desired search params
  const url = new URL(baseUrl);
  url.searchParams.append('cluster', cluster || 'devnet');
  return url.toString() + '\n';
}

export function printConsoleSeparator(message?: string) {
  console.log('\n==============================');
  console.log('==============================\n');
  if (message) console.log(message);
}