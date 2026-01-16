import {Injectable} from 'angular2/core';
import {IspitRok} from './ispit-rok';


import {Http, Response, Headers} from 'angular2/http';
import {Observable} from 'rxjs/Rx';
import {GlobalVarsService} from '../global-vars.service';

@Injectable()
export class IspitRokService{
    private urlPath='http://localhost:8080/RESTfulProject/REST/WebService/RokIspiti/';
    constructor (private _http:Http, private _gVS:GlobalVarsService){}
    
    getIspitRok (rokId:number){
        
        this.urlPath = this.urlPath + this._gVS.getStudentId().toString()+','+rokId.toString();
         return this._http.get(this.urlPath)
                    .map((res:Response) => res.json());
    }
}