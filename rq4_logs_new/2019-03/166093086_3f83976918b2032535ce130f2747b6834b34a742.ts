import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';

const httpOptions = {
  headers: new HttpHeaders({ 'JWT': localStorage.getItem('token') })
};

@Injectable({
  providedIn: 'root'
})
export class EmailService {
  //Instance variables
  private emailUrl = 'https://rapidscemeteryapi-dev.herokuapp.com/api/mail/';
  
  constructor(private http: HttpClient) { }

  /**
   * Method that will use the Angular http client to send an email
   * using the API.
   * @param data 
   */
  sendMessage(data: object): Observable<object> {
    return this.http.post<object>(this.emailUrl+'sendContactMail', data);
  }
}