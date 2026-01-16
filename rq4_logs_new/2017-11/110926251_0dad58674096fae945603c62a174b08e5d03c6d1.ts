import { Injectable } from '@angular/core';
import { Http, Response, URLSearchParams, Headers, RequestOptions } from '@angular/http';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/observable/throw';

@Injectable()
export class UserService {
    HttpUrl = "http://localhost:8000";
    constructor(
        private http: Http,
    ) { }

    login(user): Observable<any> {
        const urlSearchParams = new URLSearchParams();
        // urlSearchParams.append('content', params.content);
        // urlSearchParams.append('isLogin', isLogin);
        urlSearchParams.append('phone', user.phone);
        urlSearchParams.append('password', user.password);
        return this.http.post(this.HttpUrl + '/user/login', urlSearchParams)
            .map(this.extractData)
            .catch(this.handleError);
    }

    register(user): Observable<any> {
        const urlSearchParams = new URLSearchParams();
        urlSearchParams.append('phone', user.phone);
        urlSearchParams.append('password', user.password);
        return this.http.post(this.HttpUrl + '/user/register', urlSearchParams)
            .map(this.extractData)
            .catch(this.handleError);
    }

    extractData(res: Response) {
        return res.text() ? res.json() : {};
    }

    handleError(error: Response | any) {
        return Observable.throw(error);
    }
}