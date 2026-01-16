import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Detalleservicio } from '../interfaces/detalleservicio';
import { Listarservicios } from '../interfaces/listarservicios';

@Injectable({
  providedIn: 'root'
})
export class GestionServicioService {

  readonly rootURL = 'https://localhost:44310/api';

  listaServicios:Listarservicios[];
  detalleservicio:Detalleservicio;

  constructor(private http: HttpClient) { }

  listarServicios(){
    this.http.get(this.rootURL + '/Servicios')
    .toPromise().then(res => this.listaServicios = res as Listarservicios[]);
  }

  detalleServicio(id: number){
    this.http.get(this.rootURL + '/Servicios/' + id)
    .toPromise()
    .then(res => this.detalleservicio = res as Detalleservicio);
  }
}