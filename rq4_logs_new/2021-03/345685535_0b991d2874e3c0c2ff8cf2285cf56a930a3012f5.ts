import { HttpClient, HttpClientModule } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class LocationService {

  apiUrl: string = 'http://localhost:3000/locations';
  list$: BehaviorSubject<Location[]> = new BehaviorSubject<Location[]>([]);

  constructor(private http: HttpClient) { }


  getAll(): void {
    this.http.get<Location[]>(this.apiUrl).subscribe(
      locations => this.list$.next(locations)
    );
  }
  get(id: number): Observable<Location> {
    id = typeof id === 'string' ? parseInt(id, 10) : id;
    const ev: Observable<Location> | undefined = this.http.get<Location>(`${this.apiUrl}/${id}`);
    if (id == 0) {
      return of(new Location());
    } else {
      return ev;
    }
  }


}