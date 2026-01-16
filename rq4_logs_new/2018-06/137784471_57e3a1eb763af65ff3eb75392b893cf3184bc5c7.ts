import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { RequestOptions } from '@angular/http';
import { map } from 'rxjs/operators';

import { User } from '../models';

@Injectable()
export class TransportationService {
    constructor(private http: HttpClient) { }

    getTransportationFootprint(user: User) {
        return this.http.get('http://localhost:8080/transportation/getTransportationCO2/' + user, { // fix endpoint
            params: {}
        }).pipe(map(result => {
            return result
        }));
    }
}