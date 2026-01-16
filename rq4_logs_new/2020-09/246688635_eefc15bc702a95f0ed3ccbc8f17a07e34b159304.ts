import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { AppConfig } from '../config/app-config';
import { Task } from '../models/task';
import { Response } from 'src/app/models/response';

@Injectable({
  providedIn: 'root',
})
export class TaskService {
  private pathAPI = this.config.get('PathAPI');

  constructor(private http: HttpClient, private config: AppConfig) {}

  postTask(task: Task) {
    const header = new HttpHeaders({ 'Content-Type': 'application/json' });
    return this.http.post<Response>(`${this.pathAPI}/tasks/`, task, {
      headers: header,
    });
  }
}