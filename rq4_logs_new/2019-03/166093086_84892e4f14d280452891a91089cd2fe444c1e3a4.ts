import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Stop } from '../models/stop';

const httpOptions = {
  headers: new HttpHeaders({ 'JWT': localStorage.getItem('token') })
};

@Injectable({
  providedIn: 'root'
})
export class StopService {
  //Instance variables
  private itemsUrl = 'https://rapidscemeteryapi-dev.herokuapp.com/api/stop/';

  constructor(private http: HttpClient) { }

  /**
   * Method that will use the Angular http client
   * to retrieve data from the API.
   */
  getStops(): Observable<Stop[]> {
    return this.http.get<Stop[]>(this.itemsUrl+'getAllStops');
  }
}