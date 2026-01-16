import jwt, {SignOptions, VerifyOptions} from "jsonwebtoken";

export interface ISigner {
    verify<payloadObject>(token: string, verifyOption: VerifyOptions): Promise<payloadObject>;

    sign(payload: any, signOption: SignOptions): string;
}

export class JWTSigner implements ISigner {
    private readonly jwtSecret: string;

    constructor(secret: string) {
        this.jwtSecret = secret;
    }

    verify<payloadObject>(token: string, verifyOption: VerifyOptions): Promise<payloadObject> {
        return new Promise((resolve, reject) => {
            jwt.verify(token, this.jwtSecret, verifyOption, (err, decoded) => {
                if (err) reject(err);
                resolve(decoded as payloadObject)
            });
        })

    }

    sign(payload: any, signOption: SignOptions): string {
        return jwt.sign(payload, this.jwtSecret, signOption)
    }
}