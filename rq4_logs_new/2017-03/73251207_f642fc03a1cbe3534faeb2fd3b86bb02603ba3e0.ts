import { Injectable } from '@angular/core';
import { Http, Response, Headers, RequestOptions } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import { Storage } from '@ionic/storage';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

@Injectable()
export class AuthService {

    constructor(public storage: Storage, public http: Http) {

    }

    getToken(username: string, password: string): Observable<any> {
        //HACK hardcoding loginData for testing
        var loginData = { grant_type: 'password', username: 'Admin1', password: 'password123' };
        let headers = new Headers({ 'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8' });
        let options = new RequestOptions({ headers: headers });

        return this.http.post('http://localhost:61180/Token', loginData, options);
    }

    logout() {
        let headers = new Headers({ 'Authorization': 'Bearer auth-token' })
        let options = new RequestOptions({ headers: headers });

        return this.http.post('http://localhost:61180/Token', '', options);
    }



}