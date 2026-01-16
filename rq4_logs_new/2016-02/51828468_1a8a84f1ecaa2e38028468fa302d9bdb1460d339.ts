/// <reference path="firebase/firebase.d.ts" />

import {Injectable} from 'angular2/core';
import {Observable} from 'rxjs/Observable';
import {ReplaySubject} from 'rxjs';

import {FIREBASE_URL} from './firebase/config';

@Injectable()
class AuthService {

    private _ref: Firebase;
    private _user: Observable<any>;

    constructor() {
        this._ref = new Firebase(FIREBASE_URL);

        this._user = this.onAuth();
    }

    get user(): Observable<any> {
        return this._user;
    }

    private onAuth(): Observable<any> {
        // Use replaySubject because we are only interested in the latest value
        let res = new ReplaySubject(1);

        function listenAuth(authData) {
            res.next(authData);
        };

        this._ref.onAuth(listenAuth);

        // We never stop listening

        return res;
    }

}