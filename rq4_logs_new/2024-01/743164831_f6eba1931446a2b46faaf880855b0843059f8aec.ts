import { Injectable } from '@angular/core';
import { Message } from './model/message';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { MessageResponse } from './model/message-response';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  private apiUrl = 'http://localhost:4001';

  constructor(private http: HttpClient) {}

  sendMessages(messages: Message[]): Observable<MessageResponse[]>  {
    return this.http.post<MessageResponse[]>(`${this.apiUrl}/message`, messages);
  }

  getMessage(id: string): Observable<Message> {
    return this.http.get<Message>(`${this.apiUrl}/message/${id}`);
  }
}