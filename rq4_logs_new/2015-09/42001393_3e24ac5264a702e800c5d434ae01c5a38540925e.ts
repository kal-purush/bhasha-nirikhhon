import "typed-node";

export interface CredentialDetails {
	pfx: string;
	key: string;
	passphrase: string;
	cert: string;
	ca: any;    //string | string array
	crl: any;   //string | string array
	ciphers: string;
}
export interface Credentials { context?: any; }
export function createCredentials(details: CredentialDetails): Credentials;
export function createHash(algorithm: string): Hash;
export function createHmac(algorithm: string, key: string): Hmac;
export function createHmac(algorithm: string, key: Buffer): Hmac;
interface Hash {
	update(data: any, input_encoding?: string): Hash;
	digest(encoding: 'buffer'): Buffer;
	digest(encoding: string): any;
	digest(): Buffer;
}
interface Hmac {
	update(data: any, input_encoding?: string): Hmac;
	digest(encoding: 'buffer'): Buffer;
	digest(encoding: string): any;
	digest(): Buffer;
}
export function createCipher(algorithm: string, password: any): Cipher;
export function createCipheriv(algorithm: string, key: any, iv: any): Cipher;
interface Cipher {
	update(data: Buffer): Buffer;
	update(data: string, input_encoding?: string, output_encoding?: string): string;
	final(): Buffer;
	final(output_encoding: string): string;
	setAutoPadding(auto_padding: boolean): void;
}
export function createDecipher(algorithm: string, password: any): Decipher;
export function createDecipheriv(algorithm: string, key: any, iv: any): Decipher;
interface Decipher {
	update(data: Buffer): Buffer;
	update(data: string, input_encoding?: string, output_encoding?: string): string;
	final(): Buffer;
	final(output_encoding: string): string;
	setAutoPadding(auto_padding: boolean): void;
}
export function createSign(algorithm: string): Signer;
interface Signer extends WritableStream {
	update(data: any): void;
	sign(private_key: string, output_format: string): string;
}
export function createVerify(algorith: string): Verify;
interface Verify extends WritableStream {
	update(data: any): void;
	verify(object: string, signature: string, signature_format?: string): boolean;
}
export function createDiffieHellman(prime_length: number): DiffieHellman;
export function createDiffieHellman(prime: number, encoding?: string): DiffieHellman;
interface DiffieHellman {
	generateKeys(encoding?: string): string;
	computeSecret(other_public_key: string, input_encoding?: string, output_encoding?: string): string;
	getPrime(encoding?: string): string;
	getGenerator(encoding: string): string;
	getPublicKey(encoding?: string): string;
	getPrivateKey(encoding?: string): string;
	setPublicKey(public_key: string, encoding?: string): void;
	setPrivateKey(public_key: string, encoding?: string): void;
}
export function getDiffieHellman(group_name: string): DiffieHellman;
export function pbkdf2(password: string, salt: string, iterations: number, keylen: number, callback: (err: Error, derivedKey: Buffer) => any): void;
export function pbkdf2(password: string, salt: string, iterations: number, keylen: number, digest: string, callback: (err: Error, derivedKey: Buffer) => any): void;
export function pbkdf2Sync(password: string, salt: string, iterations: number, keylen: number) : Buffer;
export function pbkdf2Sync(password: string, salt: string, iterations: number, keylen: number, digest: string) : Buffer;
export function randomBytes(size: number): Buffer;
export function randomBytes(size: number, callback: (err: Error, buf: Buffer) =>void ): void;
export function pseudoRandomBytes(size: number): Buffer;
export function pseudoRandomBytes(size: number, callback: (err: Error, buf: Buffer) =>void ): void;