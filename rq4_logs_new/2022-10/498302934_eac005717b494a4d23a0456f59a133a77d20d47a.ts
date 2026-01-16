import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { environment } from "src/environments/environment";
import { Product } from "../model/product";

@Injectable({
  providedIn: 'root'
})
export class ProductService {
  private apiUrl = `${environment.apiUrl}/products`;

  constructor(private httpClient: HttpClient) { }
  
  getProducts(): Observable<Product[]> {
    const productsUrl = `${this.apiUrl}/`;
    return this.httpClient.get<Product[]>(productsUrl);
  }
}