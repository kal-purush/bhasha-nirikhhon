/// <reference path='../../typings/index.d.ts'/>

import { inject, injectable } from 'inversify';
import { Express, Request, Response } from 'express-serve-static-core';

import { IBrandManager } from '../managers/_namespace';
import { IBrandService } from './_namespace';

@injectable()
export class BrandService implements IBrandService {

    private _brandManager: IBrandManager;

    constructor (
        @inject('IBrandManager') brandManager: IBrandManager
    ) {
        this._brandManager = brandManager;
    }

    public registerRoutes (app: Express) {
        app.get('/brands/:id', this._getById.bind(this));
    }

    private _getById(req: Request, res: Response): void {
        let requestedId = parseInt(req.params.id, 10);

        res.send(this._brandManager.getById(requestedId));
    }

}