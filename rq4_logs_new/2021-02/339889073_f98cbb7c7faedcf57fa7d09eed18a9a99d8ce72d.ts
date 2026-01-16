import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { environment } from 'src/environments/environment';
import { ISale } from '../models/sale';

@Injectable({
  providedIn: 'root'
})
export class SalesService {

  constructor(private http: HttpClient) { }


  createSale(sale: any) {
    return this.http.post(`${environment.apiUrl}/sale`, sale)
  }

  getSales(): Observable<ISale[]> {
    return this.http.get(`${environment.apiUrl}/sale`).pipe(
      map((res: any) => {
        if (!res) {
          throw new Error('Value expected!');
        }
        return res
      })
    )
  }
}