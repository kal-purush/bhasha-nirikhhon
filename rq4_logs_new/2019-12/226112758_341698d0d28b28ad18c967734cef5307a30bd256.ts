import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { ResultVO, TableQueryParams } from "../model/result-vm";
import { Observable, of } from "rxjs";
import { ProductVO } from "../model/product";
import {AuthVO} from "../model/auth";

@Injectable({
  providedIn: 'root'
})
export class ProductService {

  constructor(
    private http: HttpClient
  ) {

  }

  findAll(queryParams: object): Observable<ResultVO<AuthVO[]>> {
    return of({
      code: 200,
      message: '',
      data: [{
        id: 1,
        description: '订单',
        route:'只读'
      }]
    })
  }

  findAllByPage(queryParams: TableQueryParams): Observable<ResultVO<AuthVO>> {
    return of({
      code: 200,
      message: '',
      data: null
    });
  }

}