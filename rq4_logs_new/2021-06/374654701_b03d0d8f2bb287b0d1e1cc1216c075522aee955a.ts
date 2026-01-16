import { Injectable } from '@angular/core';
import { ApiService } from './api.service';

@Injectable({
  providedIn: 'root'
})
export class UtenzaService {

  public IdVisualizza:any=0;
  public ControlloAdmin:any=0;
  public Ruolo:any="responsabile";
  public Nome:any="";
 
  public path: string = "utente";
 
  constructor(public api: ApiService) {}
 
  
  public LoginToken(body:any) {
    return this.api.post(this.path + "/login/", body);
  }
  public GetControllo() {
    return this.ControlloAdmin;
  }
 
  public SetControllo(nome:any) {
    this.ControlloAdmin=nome;
  }
}