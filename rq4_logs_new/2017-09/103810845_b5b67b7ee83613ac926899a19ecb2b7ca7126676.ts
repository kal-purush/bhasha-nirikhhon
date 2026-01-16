import { Http, Response, Headers } from '@angular/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/do';

@Injectable()
export class SalesReportService {

  private _url = 'http://localhost:57483/api/'
  private name;

  constructor(private http: Http) { }

  create(body) {
    // console.log(body);
        return this.http.post(this._url + 'SalesOrderHeaders/', body)
        .subscribe(response => {
          console.log(response.json);
        });
        // .map((res: Response) => res.json());
      }
  

  private logResponse(res: Response) {
    console.log(res);
  }

  private extractData(res: Response) {
    return (res: Response) => res.json();
  }

}