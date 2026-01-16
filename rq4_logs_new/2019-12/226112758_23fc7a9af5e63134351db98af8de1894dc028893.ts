import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { ResultVO, TableQueryParams, TableResultVO } from "../model/result-vm";
import { CustomerVO } from "../model/customer";

@Injectable({
  providedIn: 'root'
})
export class CustomerService {

  constructor(
    private http: HttpClient
  ) {

  }

  public find(id: number): Observable<ResultVO<CustomerVO>> {
    return of({
      code: 200,
      message: '',
      data: {
        id: 1,
        name: 'XXX公司',
        shorthand: '',
        type: 1,
        period: 1,
        payDate: 20,
        description: '',

        specialPrices: [{
          id: 0,
          productId: 0,
          productName: '铝棒',
          price: 1000,
          unit: 1
        }, {
          id: 2,
          productId: 2,
          productName: '铝条',
          price: 2000,
          unit: 2
        }]
      }
    });
  }

  public findAll(queryParams: TableQueryParams): Observable<ResultVO<TableResultVO<CustomerVO>>> {
    return of({
      code: 200,
      message: '',
      data: {
        totalPages: 1,
        pageIndex: 1,
        pageSize: 1,
        result: [{
          id: 1,
          name: 'XXX公司',
          shorthand: '',
          type: 1,
          period: 1,
          payDate: 20,
          description: ''
        }]
      }
    });
  }

  public save(customer: CustomerVO): Observable<ResultVO<any>> {
    return of(null);
  }

}