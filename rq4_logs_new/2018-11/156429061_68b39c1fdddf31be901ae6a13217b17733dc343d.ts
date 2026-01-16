import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Stock } from '../models/stock-model';

@Injectable({
  providedIn: 'root'
})
export class StockService {
  selectedStock: Stock;
  stockLst: Stock[];
  readonly URL_API = 'http://localhost:3000/api/stock'

  constructor(private http: HttpClient) { 
    this.selectedStock = new Stock();
  }

  getStockLst(){
    return this.http.get(this.URL_API);
  }

  postStock(stock: Stock){
    return this.http.post(this.URL_API, stock);
  }

  putStock(stock: Stock){
    return this.http.put(this.URL_API + `/${stock._id}`, stock);
  }

  deleteStock(_id: string){
    return this.http.delete(this.URL_API + `/${_id}`);
  }
}