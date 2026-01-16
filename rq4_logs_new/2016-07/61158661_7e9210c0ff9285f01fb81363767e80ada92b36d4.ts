/// <reference path="../../typings/index.d.ts"/>

import { inject, injectable } from "inversify";
import { Express } from "express-serve-static-core";

import { IUserManager } from "../managers/_namespace";
import { IUserService } from "./_namespace";

@injectable()
export class UserService implements IUserService {

    private _userManager: IUserManager;

    constructor (
        @inject("IUserManager") userManager: IUserManager
    ) {
        this._userManager = userManager;
    }

    public registerRoutes (app: Express) {
        app.get('/users/current', this._loginCheck.bind(this), this._getCurrent.bind(this));
    }

    private _getCurrent (req: any, res: any): any {
        let id = req.user.id;

        res.send(this._userManager.getById(id));
    }

    private _loginCheck (req: any, res: any, next: any): any {
        // if user is authenticated in the session, carry on
        if (req.isAuthenticated() && req.user !== undefined)
            return next();

        // if they aren't redirect them to the home page
        res.sendStatus(401);
    }

}