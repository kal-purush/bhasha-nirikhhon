import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';
import { Candidature } from './candidature.model';


@Injectable({
  providedIn: 'root'
})
export class CandidatureService {

  private baseUrl = environment.wsRootUrl;

  optionRequete = {
    headers: new HttpHeaders({
      'Access-Control-Allow-Origin':'*'
    })
  };

  constructor(private http: HttpClient) { }
  public getAllCandidaturesByCandidatId(idCandiat : number):Observable<Candidature[]>{
    return this.http.get<Candidature[]>(this.baseUrl+'/candidatures/candidat/'+idCandiat);
  }

  
}