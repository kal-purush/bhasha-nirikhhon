import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import 'rxjs/add/operator/map';

import { IShow } from '../models';
import { Observable } from 'rxjs/Observable';


@Injectable()
export class ShowsService {
    public api = 'http://localhost:3000/api/shows';

    constructor (public http: HttpClient) {
    }

    shows: Array<any> = [];

    getShows(): Observable<any> {
        return this.http.get(this.api);
    }

    createShows(): Observable<any> {
        return this.http.post(this.api, this.shows);
    }
}