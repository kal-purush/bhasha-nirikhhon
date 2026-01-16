import {Injectable} from 'angular2/core';
import {Ispit} from './ispit';


import {Http, Response, Headers,RequestOptions} from 'angular2/http';
import {Observable} from 'rxjs/Rx';
import {GlobalVarsService} from '../global-vars.service';

@Injectable()
export class IspitService{
    private urlPath='http://localhost:8080/RESTfulProject/REST/WebService/ispiti/';
    constructor (private _http:Http, public _gVS : GlobalVarsService){}
    
     pushIspit(predmet:number, rok:number, ocena:number, datum:string){
         console.log('u push ispit');
        let body = "studentId="+this._gVS.getStudentId().toString()+"&predmet="+predmet
                    +"&rok="+rok+"&ocena="+ocena+"&datum="+datum;
                  //let body = "id=9807&ime=ajsad&prezime=heellooo&adresa=adresa";
        console.log(this.urlPath,body);
        let headers = new Headers({ 'Content-Type': 'application/x-www-form-urlencoded' });
        let options = new RequestOptions({ headers: headers });

        return this._http.put(this.urlPath, body, options)
                    .map((res:Response) => res.json());
    }
}