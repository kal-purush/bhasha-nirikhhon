import { Injectable } from '@angular/core';
import {environment} from '../../environments/environment';

@Injectable()
export class ConnectionService {

  private queryJoin: string = ""
  private querySelect: string = ""
  constructor() { }

  getConnections(): any{
    this.queryJoin = "expand=TipoConexion"
    this.querySelect = "select=*,TipoConexion/*"
    return fetch(`${environment.api}/Conexiones?$${this.queryJoin}&$${this.querySelect}`,{
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      }
    })
    .then(response => {
      return response.json()
    })
    .catch(err => {
      console.log(err)
      throw err;
    })
  }
  getTypeConnections():any{
    return fetch(`${environment.api}/TipoConexiones`,{
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      }
    })
    .then(response => {
      return response.json()
    })
    .catch(err => {
      console.log(err)
      throw err;
    })
  }
  createConnection(connection: object): void{

    fetch(`${environment.api}/Conexiones`, {
      method: "POST",
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(connection)
    })
    .then(response => {
      if(response.ok){
        console.log("Connection creada correctamente")
      }
    })
    .catch(err => {
      console.log(err)
      throw err;
    })
  }

}