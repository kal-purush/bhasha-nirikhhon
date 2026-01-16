import * as Keys from './keys';

let Crypto: any;
export function hexStringToUint8Array(hexString: string): Uint8Array {
    return new Uint8Array(hexString.match(/.{1,2}/g)!.map(byte => parseInt(byte, 16)));
};
export async function encryptSharedMessage(ourPrivateKey: string, theirPublicKey: string, text: string): Promise<string> {
    const sharedSecret = Keys.getSharedSecret(ourPrivateKey, theirPublicKey);
    const sharedX = hexStringToUint8Array(sharedSecret.slice(2));

    let encryptedMessage: string;
    let ivBase64: any;
    if (typeof window !== "undefined"){
        const iv = crypto.getRandomValues(new Uint8Array(16));
        const key = await crypto.subtle.importKey('raw', sharedX, { name: 'AES-CBC' }, false, ['encrypt']);
        const encryptedBuffer = await crypto.subtle.encrypt({ name: 'AES-CBC', iv }, key, new TextEncoder().encode(text));
        encryptedMessage = btoa(String.fromCharCode(...new Uint8Array(encryptedBuffer)));
        ivBase64 = btoa(String.fromCharCode(...iv));
    }
    else {
        if (!Crypto){
            // @ts-ignore
            Crypto = require('crypto');
        };
        const iv = Crypto.randomBytes(16);
        const cipher = Crypto.createCipheriv('aes-256-cbc', sharedX, iv);
        encryptedMessage = cipher.update(text, 'utf8', 'base64');
        encryptedMessage += cipher.final('base64');
        ivBase64 = iv.toString('base64');
    };
    return `${encryptedMessage}?iv=${ivBase64}`;
};
export async function decryptSharedMessage(ourPrivateKey: string, theirPublicKey: string, encryptedData: string): Promise<string> {
    let decryptedMessage: string = '';
    try {
        const [encryptedMessage, ivBase64] = encryptedData.split('?iv=');

        const sharedSecret = Keys.getSharedSecret(ourPrivateKey, '02' + theirPublicKey);
        const sharedX = hexStringToUint8Array(sharedSecret.slice(2));
        if (typeof window !== "undefined"){
            const iv = Uint8Array.from(atob(ivBase64), c => c.charCodeAt(0));
            const key = await crypto.subtle.importKey('raw', sharedX, { name: 'AES-CBC' }, false, ['decrypt']);
            const decryptedBuffer = await crypto.subtle.decrypt({ name: 'AES-CBC', iv }, key, Uint8Array.from(atob(encryptedMessage), c => c.charCodeAt(0)));
            decryptedMessage = new TextDecoder().decode(decryptedBuffer);
        }
        else {
            if (!Crypto){
                // @ts-ignore
                Crypto = require('crypto');
            };
            // @ts-ignore
            const iv = Buffer.from(ivBase64, 'base64');
            const decipher = Crypto.createDecipheriv('aes-256-cbc', sharedX, iv);
            // @ts-ignore
            let decrypted = decipher.update(Buffer.from(encryptedMessage, 'base64'));
            // @ts-ignore
            decrypted = Buffer.concat([decrypted, decipher.final()]);
            decryptedMessage = decrypted.toString('utf8');
        }
    }
    catch (e) {
    };
    return decryptedMessage;
};
export async function encryptWithPubKey(publicKey: string, text: string): Promise<string> {
    let ephemeralKey = Keys.generatePrivateKey();
    return (await encryptSharedMessage(ephemeralKey, publicKey, text)) + "&pbk=" + Keys.getPublicKey(ephemeralKey);
}
export async function decryptWithPrivKey(privateKey: string, encryptedData: string): Promise<string> {
    let decryptedMessage: string = '';
    try {
        let [_encryptedData, ephemeralKey] = encryptedData.split("&pbk=");
        decryptedMessage = await decryptSharedMessage(privateKey, ephemeralKey, _encryptedData);
    }
    catch (e) {
    };
    return decryptedMessage;
}