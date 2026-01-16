import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { Session } from '../models/session';

@Injectable()
export class SessionService {

    private _session = new BehaviorSubject<Session>(null);

    constructor() {}

    get sessionObservable(): Observable<any> {
        return this._session;
    }

    get sessionData(): Session {
        return this._session.getValue();
    }

    set sessionData(session: Session) {
        this._session.next(session);
    }
}