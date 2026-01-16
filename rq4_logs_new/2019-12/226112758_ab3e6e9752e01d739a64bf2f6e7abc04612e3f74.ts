import {Injectable} from "@angular/core";
import {HttpClient} from "@angular/common/http";
import {Observable, of} from "rxjs";
import {ResultVO, TableQueryParams, TableResultVO} from "../model/result-vm";
import {StringUtils} from "./util.service";
import {OperationInfoVO} from "../model/operation";

@Injectable({
  providedIn: 'root'
})
export class OperationService {

  constructor(private http: HttpClient) {

  }

  public findAll(queryParams: TableQueryParams): Observable<ResultVO<TableResultVO<OperationInfoVO>>> {
    // // prod
    // return this.http.post<ResultVO<TableResultVO<OperationInfoVO>>>(
    //   `${AppConfig.BASE_URL}/api/operation-log/list`,
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
          user_name: '殷乾恩',
          created_at: '2019-10-10 12:00',
          description: '创建了加工单XXXXXXX'
        },{
          id: 2,
          user_name: '杨关',
          created_at: '2019-10-10 12:00',
          description: '创建了加工单XXXXXXX'
        }]
      }
    });
  }

  public abandon(id: string): Observable<ResultVO<any>> {
    return of(null);
  }

}