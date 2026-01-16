import { Injectable } from '@angular/core';
import { Headers, Http } from '@angular/http';

import 'rxjs/add/operator/toPromise';


@Injectable()
export class RegisterService {

    private headers = new Headers({'Content-Type': 'application/json'});
    private postURL = 'localhost:8000';  // URL to web api

    constructor(private http: Http) { }

    addUser(name: string, email: string, password: string) {
    return this.http
        .post(this.postURL, JSON.stringify({name: name, email: email, password: password}), {headers: this.headers})
        .toPromise()
        .catch(this.handleError);
    }

    private handleError(error: any): Promise<any> {
        console.error('An error occurred', error); // for demo purposes only
        return Promise.reject(error.message || error);
    }
}