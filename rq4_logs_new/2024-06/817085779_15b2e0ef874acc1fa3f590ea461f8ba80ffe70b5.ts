import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Subject } from 'rxjs';
import { Actividad } from '../models/actividad';
import { environment } from '../../environments/environments';

const base_url = environment.base;

@Injectable({
  providedIn: 'root'
})
export class ActividadService {

  private url = `${base_url}/actividadrecreativa`;
  private listaCambio=new Subject<Actividad[]>()

  constructor(private http: HttpClient) { }

  list() {
    return this.http.get<Actividad[]>(this.url);
  }

  insert(a: Actividad) {
    return this.http.post(this.url, a);
  }

  setList(listaNueva: Actividad[]) {
    this.listaCambio.next(listaNueva);
   }

   getList() {
    return this.listaCambio.asObservable();
   }

   listId(id: number) {
    return this.http.get<Actividad>(`${this.url}/${id}`);
  }

  update(a: Actividad) {
    return this.http.put(this.url, a);
  }
  delete(id: number) {
    return this.http.delete(`${this.url}/${id}`);
  }
}