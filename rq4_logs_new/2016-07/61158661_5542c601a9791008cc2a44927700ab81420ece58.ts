/// <reference path="../../typings/index.d.ts"/>

import { inject, injectable } from "inversify";
import { Express } from "express-serve-static-core";

import { INoodleManager } from "../managers/_namespace";
import { INoodleService } from "./_namespace";

@injectable()
export class NoodleService implements INoodleService {

    private _noodleManager: INoodleManager;

    constructor (
        @inject("INoodleManager") noodleManager: INoodleManager
    ) {
        this._noodleManager = noodleManager;
    }

    public registerRoutes (app: Express) {
        app.get('/reviews/:id', this._getById.bind(this));
    }

    private _getById(req: any, res: any): any {
        let requestedId = parseInt(req.params['id']);

        res.send(this._noodleManager.getById(requestedId));
    }

}