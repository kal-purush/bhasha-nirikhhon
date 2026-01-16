import { Injectable } from "@angular/core";
import { HttpHeaders, HttpClient, HttpErrorResponse, HttpParams } from "@angular/common/http";
import { catchError } from 'rxjs/operators';
import { Constants } from "../constants/constants";

const httpOptions = {
  body: undefined,
  headers: new HttpHeaders({
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }),
  observe: 'response' as 'response'
};

@Injectable()
export class HttpService {
  private baseUrl = Constants.backend.baseUrl;

  constructor(private http: HttpClient) { }

  get(url: string) {
    return this.request('Get', this.baseUrl + url);
  }

  post(url: string, data: Object | Array<Object>) {
    return this.request('Post', this.baseUrl + url, data);
  }

  put(url: string, data: Object) {
    return this.request('Put', this.baseUrl + url, data);
  }

  patch(url: string, data: Object) {
    return this.request('Patch', this.baseUrl + url, data);
  }

  delete(url: string) {
    return this.request('Delete', this.baseUrl + url);
  }

  private request(method: string, url: string, data?: Object | Array<Object>) {

    if (data) {
      httpOptions.body = data;
    }

    return this.http.request(method, url, httpOptions)
    .pipe(
    );
  }

}