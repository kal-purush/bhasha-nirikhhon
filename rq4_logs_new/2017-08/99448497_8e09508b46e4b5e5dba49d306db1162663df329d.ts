/// <reference types="node-forge" />
import * as forge from 'node-forge';
export interface IApnsEnvironment {
    sandbox: boolean;
    production: boolean;
}
export declare class ApnsCertificate {
    readonly key: string;
    readonly cert: string;
    readonly passphrase?: string;
    readonly environment: IApnsEnvironment;
    readonly topic: string;
    readonly expires: Date;
    constructor(bytes: forge.Base64, passphrase?: string);
    private decryptPkcs12(bytes, passphrase?);
    private getPrivateKey(p12);
    private getCertEnvironment(cert);
    private getCertificate(cert);
}