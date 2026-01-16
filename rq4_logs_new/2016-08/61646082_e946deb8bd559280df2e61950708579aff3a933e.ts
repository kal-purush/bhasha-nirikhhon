import {Injectable} from '@angular/core';
import {Http, Headers, Response, RequestOptions} from '@angular/http';
import {contentHeaders} from '../common/headers';
import {Observable} from "rxjs/Rx";
import {AuthHttp} from 'angular2-jwt';

@Injectable()
export class CalendarService {

    private actionUrl: string;
    private headers: Headers;

    private options = new RequestOptions({
        body: '',
        headers: contentHeaders,
    });

    constructor(private _http: Http, private authHttp: AuthHttp) {
        this.actionUrl = process.env.BASE_URL + process.env.API_PATH;
        this.headers = contentHeaders;
    }

    public getAll(): Observable<Response> {
        return this.authHttp.get(this.actionUrl + "calendar/year/2016", this.options);
    }
}