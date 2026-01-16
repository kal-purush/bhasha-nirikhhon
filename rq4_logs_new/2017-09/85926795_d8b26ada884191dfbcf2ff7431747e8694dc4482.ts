import { Injectable }               from '@angular/core';
// net zoals in Postman, HTTP request levert een response op:
import { Http, Response, Headers }  from '@angular/http';
import { Observable }               from 'rxjs/Observable';

import { Docent }                 from './docent';
import { DocentComponent }          from './docent.component';
import { AppGlobalService }         from '../app.global.service';

@Injectable()
export class DocentaanmakenService {

  constructor(private http: Http, private appGlobalService: AppGlobalService) { }

  private baseUrl: string = this.appGlobalService.baseUrl + "/docent";
  private headers = new Headers({ 'Content-Type': 'application/json' });
  //private headersXml          = new Headers({ 'Content-Type': 'text/xml' });          //waarschijnlijk niet nodig  
  //private headersImg          = new Headers({ 'Content-Type': 'text/plain' });        //waarschijnlijk niet nodig

  postDocent(docent: Docent) {
    console.log("docentaanmaken.service - postDocent");
    return this.http.post(this.baseUrl, JSON.stringify(docent), { headers: this.headers }).map(res => res.text());
  }
}