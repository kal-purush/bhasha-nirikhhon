import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

import { API_URL } from '../../../environments/environment';

@Injectable()
export class GamerInfoService {
    constructor(private http: Http) { }

    get(gamer) {
        const url = `${API_URL}/gamerinfo`;

        return this.http
                   .get(url)
                   .map( (response: Response) => response.json())
                   .catch( (error: any) => Observable.throw(error));
    }
}