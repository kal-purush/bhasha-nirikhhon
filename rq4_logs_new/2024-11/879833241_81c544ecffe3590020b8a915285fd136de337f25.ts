import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Reserva } from '../interface/reserva';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ReservasService {
  urlBase: string = "http://localhost:3000/reservas"
  constructor(private http:HttpClient) { }


  getReserva(): Observable<Reserva[]>{
    return this.http.get<Reserva[]>(this.urlBase);
  }


  postReserva(reserva:Reserva): Observable<Reserva>{
    return this.http.post<Reserva>(this.urlBase,reserva);
 }


}