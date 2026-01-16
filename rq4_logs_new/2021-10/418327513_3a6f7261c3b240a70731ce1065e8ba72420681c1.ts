import express from "express";
import {AuthProvider, Principal} from "../../../../core/infra/BaseController";
import {inject, injectable} from "inversify";
import {TYPES} from "../../../../infra/inversify/config/types";
import {ISigner} from "../../services/Authorization/interfaces/ISigner";
import {VerifyErrors} from "jsonwebtoken";


class IdentityPrincipal implements Principal {
    private readonly isAuth: boolean;
    details: any;

    constructor(isAuth: boolean, details?: any) {
        this.isAuth = isAuth
        this.details = details;
    }

    isAuthenticated(): Promise<boolean> {
        return Promise.resolve(this.isAuth);
    }

    isInRole(role: string): Promise<boolean> {
        return Promise.resolve(false);
    }

    isResourceOwner(resourceId: any): Promise<boolean> {
        return Promise.resolve(false);
    }

}

@injectable()
export class IdentityAuthProvider implements AuthProvider {
    @inject(TYPES.ISigner) private readonly jwtSigner: ISigner;

    async getUser(req: express.Request, res: express.Response): Promise<Principal> {
        return new Promise(async (resolve) => {
            const authHeader: string = req.headers.authorization
            if (!!authHeader === true) {
                const auth = authHeader.split(' ');
                const authType = auth[0]
                const accessToken = auth[1]
                console.log(authType)
                console.log(accessToken)
                if (authType === 'Bearer') {
                    try {
                        const payload = await this.jwtSigner.verify(accessToken)
                        resolve(new IdentityPrincipal(true, payload));
                    } catch (err) {
                        console.log(`verify error ${err}`)
                        resolve(new IdentityPrincipal(false));
                    }
                }
            }
            resolve(new IdentityPrincipal(false))
        })

    }
}