import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { Donateur } from './donateur';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class UtilisateurService {

  private BackUrl: string =environment.wsRootUrl;

  optionRequete = {
    headers: new HttpHeaders({
      'Access-Control-Allow-Origin':'*'
    })
  };

  constructor(private http: HttpClient) {}

  createDonateur(donateur: Donateur): Observable<Donateur>{
    let endPoint: string="/donateurs";
    return this.http.post<Donateur>(this.BackUrl + endPoint, donateur, this.optionRequete);
  }
}