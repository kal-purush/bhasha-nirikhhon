import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { ResultCode, ResultVO, TableQueryParams, TableResultVO } from "../model/result-vm";
import { SummaryProductVO, SummaryVO } from "../model/summary";

@Injectable({
  providedIn: 'root'
})
export class SummaryService {

  constructor(
    private http: HttpClient
  ) {

  }

  public getSummary(): Observable<ResultVO<SummaryVO>> {
    return of({
      code: ResultCode.SUCCESS.code,
      message: '',
      data: {
        processingOrderTotalWeight: 1000,
        shippingOrderTotalWeight: 1000,
        shippingOrderTotalCash: 1000,
        totalReceivedCash: 1000,
        totalOverdueCash: 1000,
        purchaseOrderTotalUnpaidCash: 1000,
        processingOrderTotalNum: 1000,
        shippingOrderTotalNum: 1000,

        averagePriceMonthly: 1000,
        averagePriceCash: 1000
      }
    });
  }

  public getSummaryProducts(queryParams: TableQueryParams): Observable<ResultVO<TableResultVO<SummaryProductVO>>> {
    return of({
      code: ResultCode.SUCCESS.code,
      message: '',
      data: {
        pageIndex: 1,
        pageSize: 10,
        totalPages: 1,
        result: [{
          id: 1,
          name: 'XX商品',
          totalWeight: 1000,
          averagePrice: 1000
        }]
      }
    });
  }

}