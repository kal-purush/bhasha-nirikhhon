import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class FilmService {

  constructor(public http: HttpClient) { }
  private baseUrlFilm = 'http://localhost:8080/movies';
  private baseUrlSchedule = 'http://localhost:8080/movieSchedules/hour';

  getAllFilm(): Observable<any> {
    return this.http.get(this.baseUrlFilm);
  }

  getFilmById(id: number): Observable<any> {
    return this.http.get(this.baseUrlFilm + '/' + id);
  }

  getScheduleById(id: number): Observable<any> {
    return this.http.get(this.baseUrlSchedule + '/' + id);
  }
  addNewFilm(film): Observable<any>{
    return  this.http.post(this.baseUrlFilm,film);
  }

  updateFilm(film,id): Observable<any>{
    return this.http.put(this.baseUrlFilm + '/'+ id,film);
  }
}