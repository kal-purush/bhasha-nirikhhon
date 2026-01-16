import {Injectable} from "@angular/core";
import {Http, Headers, Response} from "@angular/http";
import 'rxjs/Rx';
import {Observable} from "rxjs";

import {User} from "./user.model";

//Whit this line, this service is able to be injected the http object.
@Injectable()
export class AuthService {
    constructor(private http: Http) {

    }

    signup(user: User) {
        const body = JSON.stringify(user);
        const headers = new Headers({'Content-Type': 'application/json'});

        //remember that http.method(), returns an observable, it is not making the request yet!
        //that's why we return the observable.
        return this.http.post('http://localhost:3000/user', body, {headers: headers})
        //Remember that .map is called after the backend responds!!
            .map((response: Response) => response.json())
            //If any error, this is how is cached:
            .catch((error: Response) => Observable.throw(error.json()));
    }
}