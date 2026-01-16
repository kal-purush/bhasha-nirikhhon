import { Injectable } from "@angular/core";
import { Http } from "@angular/http";
import { User } from "../models/user.model";
import { Observable } from "rxjs/Observable";

@Injectable()
export class UserService {
    private baseUrl: string = 'api/turism-app'

    constructor(
        private _http: Http
    ) { }

    public addUser(newUser: User): Observable<User> {
        console.log('ceva');
        return this._http.post(`${this.baseUrl}`, newUser)
            .map(res => res.json());
            // .catch((error: any) => Observable.throw(error.json().error || 'Server error'));
    }
}