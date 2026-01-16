/// <reference path="../../typings/tsd.d.ts" />

/// <reference path="../api.d.ts" />

import {ResultDataStoreInterface} from '../datastore/DataStoreInterfaces';
import {ResultRouteControllerInterface} from './Routes';

class ResultRouteController implements ResultRouteControllerInterface {
    private api:ResultDataStoreInterface = null;

    constructor(api:ResultDataStoreInterface) {
        this.api = api;
    }

    public getResults(skip:number, limit:number, callback:(err:Error, result:ApiResponses.ResultResponse[])=>any) {
        this.api.getResults(skip, skip + limit, callback);
    }
}

export = ResultRouteController;