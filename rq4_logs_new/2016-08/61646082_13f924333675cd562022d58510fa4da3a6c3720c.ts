import {Injectable} from '@angular/core';
import {Http, Headers, Response} from '@angular/http';
import {contentHeaders} from '../common/headers';
import {Observable} from "rxjs/Rx";

@Injectable()
export class CalendarService {

    private actionUrl:string;
    private headers:Headers;

    constructor(private _http:Http) {
        this.actionUrl = process.env.BASE_URL + process.env.API_PATH;
        this.headers = contentHeaders;
    }

    public getAll():Observable<Response> {
        return this._http.get(this.actionUrl + "calendar/year/2016", {headers: this.headers});
    }
}