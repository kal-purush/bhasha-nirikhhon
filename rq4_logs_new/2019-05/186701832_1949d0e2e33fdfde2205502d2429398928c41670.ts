import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Subject, Observable } from 'rxjs';
import { environment } from '../../environments/environment';


import { Vehicule } from '../models/vehicule';

@Injectable({
  providedIn: 'root'
})
export class DataService {

  private subject = new Subject<Vehicule>();
  constructor( private _http: HttpClient) { }

  listeVehicules() :Observable<Vehicule[]> {
    return this._http.get<Vehicule[]>(`${environment.baseUrl}admin/vehicules`, {withCredentials:true});
  }


}