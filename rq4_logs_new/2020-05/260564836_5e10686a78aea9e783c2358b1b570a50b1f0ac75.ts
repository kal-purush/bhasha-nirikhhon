import {map} from 'rxjs/operators';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Injectable} from '@angular/core';

const httpHeaders = new HttpHeaders({
  'Content-Type': 'application/json',
  Authorization: localStorage.getItem('token')
});

@Injectable()
export class ApiService {

  constructor(public httpClient: HttpClient) {
  }

  public get = (url: string, path: string) => {
    return this.httpClient.get(
      url.concat(path),
      {headers: httpHeaders}
    ).pipe(map(response => {
      return response;
    }));
  }
}