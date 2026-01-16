import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import { Api } from '../api/api';

@Injectable()
export class SalesInfoProvider {

  constructor(public http: Http, public api: Api) {
    console.log('Hello SalesInfoProvider Provider');
  }

  // 获取销售信息
  query(params?: any) {
    return this.api.get('saledatainfoes', params)
      .map(resp => resp.json());
  }

  // 添加销售信息
  add(params?: any) {
    return this.api.post('saledatainfoes', params)
      .map(resp => resp.json())
  }
}