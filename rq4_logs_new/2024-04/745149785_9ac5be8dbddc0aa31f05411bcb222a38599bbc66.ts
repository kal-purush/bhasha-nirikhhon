import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { BehaviorSubject, Observable } from 'rxjs';
import { plan } from '../models/plan';
@Injectable({
  providedIn: 'root'
})
export class serviciosService {
 
  API: string ="http://localhost/plan/";

 
 
  public idService: BehaviorSubject<number> = new BehaviorSubject<number>(0);
  public seleccionado: BehaviorSubject<number> = new BehaviorSubject<number>(0);
  public confirmButton: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(false);
 
  services: any[] = [];
  data: any = {};
  
  constructor(private clienteHttp:HttpClient) {
  }

  ///************************SERVICIOS */

  newService(data: any): Observable<any> {
    return this.clienteHttp.post(this.API+"servicesMembresia.php?insertarservicio", data);
  }

  updateService(data: any): Observable<any> {
    return this.clienteHttp.post(this.API+"servicesMembresia.php?actualizarServicio", data);
  }

  getService(id: number): Observable<any> {
    return this.clienteHttp.get(this.API+"servicesMembresia.php?getServicio="+id);
  }

  agregarServicios(datoService: any):Observable<any>{
    return this.clienteHttp.post(this.API+"servicesMembresia.php?insertar", datoService);
  }

}