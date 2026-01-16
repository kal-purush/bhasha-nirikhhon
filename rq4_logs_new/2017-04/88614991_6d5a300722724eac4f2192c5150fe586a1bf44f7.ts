import { Injectable } from '@angular/core';
import { Http, Response, RequestOptions, Headers } from '@angular/http';

import { Observable } from 'rxjs/Observable';

import 'rxjs/add/operator/map';


export type customServerResponseObject = { success: boolean, data: any, error: any };

@Injectable()
export class HttpService {

    constructor(private _http: Http) { }

    GetHeaders() {
        let headers: Headers = new Headers();
        headers.append('Content-Type', 'application/json');
        let options: RequestOptions = new RequestOptions();
        options.headers = headers;
        return options;
    }
    ResponseMap(res: Response) {
        let response: customServerResponseObject = res.json();
        if(response && response.hasOwnProperty('success')) {
            return response;
        }
        else {
            return {
                success: false,
                data: null,
                error: response
            }
        }
    }
    GetRequest(url: string): Observable<customServerResponseObject> {
        return this._http.get(url).map(this.ResponseMap);
    }
    PostRequest(url: string, obj: Object): Observable<customServerResponseObject> {
        return this._http.post(url, obj, this.GetHeaders()).map(this.ResponseMap);
    } 
    PutRequest(url: string, obj: Object): Observable<customServerResponseObject> {
        return this._http.put(url, obj, this.GetHeaders()).map(this.ResponseMap);
    }
    DeleteRequest(url: string): Observable<customServerResponseObject> {
        return this._http.delete(url).map(this.ResponseMap);
    }

}