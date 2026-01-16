import { AutenticacaoService } from './../autenticacao.service';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})

export class MateriaisService {
  constructor(private http: HttpClient, private autenticacao: AutenticacaoService) { 
    this.autenticacao.setHeader();
  }

  public get_materiais(): Observable<any> {
    return this.http.get(environment.urlApi + 'material/', this.autenticacao.httpOptions);
  }

  public delete_materiais(id): Observable<any>{
    return this.http.delete(environment.urlApi + 'material/' + id, this.autenticacao.httpOptions);
  }

  public create_materiais(material): Observable<any> {
    return this.http.post(environment.urlApi + 'material/', material, this.autenticacao.httpOptions);
  }

  public edit_materiais(id, material): Observable<any> {
    return this.http.put(environment.urlApi + 'material/' + id, material, this.autenticacao.httpOptions);
  }

  public getById(id) {
    return this.http.get(environment.urlApi + 'material/' + id, this.autenticacao.httpOptions);
  }
}