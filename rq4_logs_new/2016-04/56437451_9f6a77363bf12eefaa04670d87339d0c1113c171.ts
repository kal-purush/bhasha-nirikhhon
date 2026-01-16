import {Injectable} from 'angular2/core';
import {Odsek} from './odsek';


import {Http, Response, Headers} from 'angular2/http';
import {Observable} from 'rxjs/Rx';
import {GlobalVarsService} from '../global-vars.service';

@Injectable()
export class OdsekService{
    private urlPath='http://localhost:8080/RESTfulProject/REST/WebService/odseci/';
    constructor (private _http:Http){}
    
    getOdseke (){
         return this._http.get(this.urlPath)
                    .map((res:Response) => res.json());
    }
    
    getOdsek(odsekId: number){
        
         return this._http.get(this.urlPath+odsekId.toString())
                    .map((res:Response) => res.json());
    }
}