import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Evento } from '../Interfaces/evento';

// Definir la interfaz del evento
@Injectable({
  providedIn: 'root'
}) 
export class EventosService {
  private apiUrl = 'http://localhost:3000/api/eventos'; // URL de tu API en el backend

  constructor(private http: HttpClient) {}

  // Obtener todos los eventos
  getEventos(): Observable<Evento[]> {
    return this.http.get<Evento[]>(this.apiUrl);
  }

  // Obtener un evento por ID
  getEventoById(id: string): Observable<Evento> {
    return this.http.get<Evento>(`${this.apiUrl}/${id}`);
  }

  // Crear un nuevo evento
  createEvento(evento: Evento): Observable<Evento> {
    return this.http.post<Evento>(this.apiUrl, evento);
  }

  // Actualizar un evento existente
  updateEvento(id: string, evento: Evento): Observable<Evento> {
    return this.http.put<Evento>(`${this.apiUrl}/${id}`, evento);
  }

  // Eliminar un evento
  deleteEvento(id: string): Observable<void> {
    return this.http.delete<void>(`${this.apiUrl}/${id}`);
  }

  // Obtener solo las fechas de eventos activos
  getFechasEventos(): Observable<string[]> {
    return this.http.get<string[]>(`${this.apiUrl}/fechas`);
  }

  // Obtener eventos por estado
  getEventosByEstado(estado: string): Observable<Evento[]> {
    return this.http.get<Evento[]>(`${this.apiUrl}/estado/${estado}`);
  }

  // Método para obtener eventos próximos a la fecha actual
  getEventosProximos(): Observable<Evento[]> {
    return this.http.get<Evento[]>(`${this.apiUrl}/proximos`);
  }

  // Obtener eventos por id del cliente
  getEventosByCliente(id_del_cliente: string): Observable<Evento[]> {
    return this.http.get<Evento[]>(`${this.apiUrl}/cliente/${id_del_cliente}`);
  }
}