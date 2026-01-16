import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map, tap } from 'rxjs/operators';
import { environment } from 'src/environments/environment';
import { User } from '../models/user';
import { LocalStorageService } from '../services/local-storage.service';
import { getUserFromToken } from '../_helpers/tokenHelper';
import { Product } from '../models/product';

@Injectable({
  providedIn: 'root'
})
export class PaymentService {

  URL = environment.baseUrl + 'users';
  constructor(private http: HttpClient, private lss: LocalStorageService) { }


  getPurchases(): Observable<Product[]> {
    const user_id = getUserFromToken().user_id;
    return this.http.get<Product[]>(this.URL)
      .pipe(map(products => products.map(product => new Product(product))));
  }

  complete(session_id: string): Observable<any>{
    const user_id = getUserFromToken().user_id;
    return this.http.post<string>(`${this.URL}/complete`, { session_id }).pipe(
      map(res => (res as any).session_id))
  }

  checkout(products: any): Observable<string> {
      const user_id = getUserFromToken().user_id;
      return this.http.post<string>(`${this.URL}/${user_id}/checkout`, products).pipe(
        map(res => (res as any).session_id))
  }
}