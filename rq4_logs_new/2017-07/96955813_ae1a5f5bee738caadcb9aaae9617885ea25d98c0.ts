import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

import { Gamer } from '../../models/gamer.interface';
import { API_URL } from '../../../environments/environment';

@Injectable()
export class CharacterService {
    constructor(private http: Http) { }

    inventory(gamer: Gamer) {
        const url = `${API_URL}/localhost:3000/inventory`;
        return this.http
                   .post(url, gamer)
                   .map( (response: Response) => response.json())
                   .catch( (error: any) => Observable.throw(error));
    }

}