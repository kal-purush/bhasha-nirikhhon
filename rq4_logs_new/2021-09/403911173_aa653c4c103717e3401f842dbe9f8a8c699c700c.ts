import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class EmailNodeService {

  url = 'https://festival-de.herokuapp.com/email';

  constructor(private http: HttpClient) {}

  sendMessage(body): Observable<any> {
    const headers = {
      headers: new HttpHeaders({
        'Content-Type': 'application/json'
      })
    };
    return this.http.post(this.url, body, headers);
  }
}