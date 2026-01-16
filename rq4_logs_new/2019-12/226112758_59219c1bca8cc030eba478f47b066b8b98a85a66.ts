import {Injectable} from "@angular/core";
import {HttpClient} from "@angular/common/http";
import {Observable, of} from "rxjs";
import {ResultVO, TableQueryParams, TableResultVO} from "../model/result-vm";
import {StringUtils} from "./util.service";
import {UserManagementInfoVO} from "../model/user-management";
import {ExpenseInfoVO} from "../model/expense";

@Injectable({
  providedIn: 'root'
})
export class ExpenseService {

  constructor(private http: HttpClient) {

  }

  public findAll(queryParams: TableQueryParams): Observable<ResultVO<TableResultVO<ExpenseInfoVO>>> {
    // // prod
    // return this.http.post<ResultVO<TableResultVO<UserManagementInfoVO>>>(
    //   `${AppConfig.BASE_URL}/api/user-management/list`,
    //   queryParams
    // );
    // test
    return of({
      code: 200,
      message: '',
      data: {
        totalPages: 1,
        pageIndex: 1,
        pageSize: 10,
        result: [{
          id: 1,
          title: '水费',
          description: '每月正常支出',
          cash: 10000,
          doneAt: '2019-10-10 12:00',
        },{
          id: 2,
          title: '电费',
          description: '每月正常支出',
          cash: 30000,
          doneAt: '2019-10-11 12:00',
        }]
      }
    });
  }

  public save(info: ExpenseInfoVO): Observable<ResultVO<any>> {
    return of({
      code: 200,
      message: '',
      data: null
    });
  }

  public abandon(id: string): Observable<ResultVO<any>> {
    return of(null);
  }

}