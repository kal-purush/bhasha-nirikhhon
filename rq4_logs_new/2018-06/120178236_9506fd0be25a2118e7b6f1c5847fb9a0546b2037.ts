import {Injectable} from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import {Observable} from 'rxjs/Observable';

const httpOptions = {
  headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};

@Injectable()
export class FrontpageService {

  constructor(private http:HttpClient) {}


  sendMail(mail: string) {
    return this.http.get('https://hack-eps-api.herokuapp.com/' + mail, {responseType: 'text'});
  }
}