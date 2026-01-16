import {Injectable} from '@angular/core';
import {Http, Headers, Response} from '@angular/http';
import {contentHeaders} from '../common/headers';
import {Observable} from "rxjs/Rx";

@Injectable()
export class CalendarService {

    private actionUrl:string;
    private headers:Headers;
    private ENV = {
        environment: 'dev',
        baseUrl: 'http://localhost:8080',
        fakeBaseUrl: 'http://localhost:8081',
        apiPath: '',
        locationType: 'auto'
    }

    constructor(private _http:Http) {

        this.actionUrl = this.ENV.baseUrl + this.ENV.apiPath;
        this.headers = contentHeaders;

    }

    public getAll():Observable<Response> {
        return this._http.get(this.actionUrl + "/calendar/year/2016", {headers: this.headers});
    }
}