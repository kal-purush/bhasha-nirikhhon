import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Storage } from '@ionic/storage';

@Injectable({
  providedIn: 'root'
})
export class ForoService {

  API:String = 'http://localhost:3000/api/'
  foroAct: String;
  preguntaAct: String;

  constructor(private http: HttpClient, private storage: Storage) {}

  getForos(){
    return this.http.get(`${this.API}foro/`)
  }

  getForo() {
    return this.http.get(`${this.API}foro/one/${this.foroAct}`)
  }
}