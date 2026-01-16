import forge from 'node-forge';
import { environment } from 'src/environments/environment';

/**
 * 
 */
export function encryptRSA(text: string): string {
    const publicKey = '-----BEGIN PUBLIC KEY-----'
        + environment.rsaPublicKey
        + '-----END PUBLIC KEY-----';

    const rsa = forge.pki.publicKeyFromPem(publicKey);
    const encryptedBytes = rsa.encrypt(forge.util.encodeUtf8(text), 'RSAES-PKCS1-V1_5');
    return forge.util.encode64(encryptedBytes);
}