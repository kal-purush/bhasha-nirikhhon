import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import 'rxjs/add/operator/map'

import { Race } from './race.model';

@Injectable()
export class RaceService {

  private racesUrl = 'assets/races.json'

  constructor(private http: Http) { }

  getAll(): Observable<Race[]> {
    return this.http.get(this.racesUrl)
      .map((res: Response) => res.json());
  }

  getByID(id: number): Observable<Race> {
    return this.getAll().map(all => {
      return all.find(e => e.id === id);
    });
  }
}
