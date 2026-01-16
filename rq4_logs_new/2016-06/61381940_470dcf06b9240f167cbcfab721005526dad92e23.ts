import {Http, URLSearchParams} from '@angular/http';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/operator/map';

import {API_ENDPOINT, API_EXTENSION} from '../../constants';
import {ConfRoom} from '../models/conf-room';

export class ConfRooms {
  private url: string = '';
      
  constructor(private http: Http) {
    this.url = API_ENDPOINT + 'confroom' + API_EXTENSION;
  }

  public get(): Observable<Array<ConfRoom>> {   
    return this.http.get(this.url)
      .map(res => res.json());
  }
}