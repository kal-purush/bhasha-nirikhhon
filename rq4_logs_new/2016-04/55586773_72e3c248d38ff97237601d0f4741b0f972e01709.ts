/**
 * Created by cChariereFiedler on 06/04/2016.
 */
import {Injectable} from 'angular2/core';
import {Http, Headers} from 'angular2/http';

import {Constants} from "../utils/Constants";
import {Domain} from "./../models";
import {BaseService} from "./BaseService";



@Injectable()
export class RoundService extends BaseService<Domain> {
    constructor(http:Http) {
        super(http);
    }
    protected getUrl():string {
        return Constants.DOMAIN_URL;
    }

    protected getId(entity:Domain):string {
        return entity.id;
    }


}