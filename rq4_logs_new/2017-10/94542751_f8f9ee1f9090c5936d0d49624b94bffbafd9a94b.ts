import { Injectable } from '@angular/core';
import { Http, Headers, RequestOptions, Response } from '@angular/http';
import { BaseService } from './baseService';

@Injectable()
export class AnalysisService extends BaseService {
    constructor(public http: Http) {
      super(http);
    }

    public async getRuleList() {
      let ruleList = null;
      const opt = await super.getOptions();

      await this.http.get(this.baseUrl + '/rule', opt).toPromise().then((data) => {
        const resObject = data.json();
        console.log(resObject);
        ruleList = resObject;
      });
      return ruleList;
    }
}