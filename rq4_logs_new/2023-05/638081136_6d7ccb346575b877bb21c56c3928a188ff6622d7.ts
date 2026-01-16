import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Task } from '../Task';
import { Config } from '../config';

@Injectable({
  providedIn: 'root',
})
export class TaskRepository {
  private config = new Config();

  constructor(private http: HttpClient) {}

  getAllTasks(): Observable<Task[]> {
    return this.http.get<Task[]>(this.config.apiUrl);
  }

  deleteTask(task: Task): Observable<void> {
    const urlWithId = `${this.config.apiUrl}/${task.id}`;
    return this.http.delete<void>(urlWithId);
  }

  updateTask(task: Task): Observable<Task> {
    const urlWithId = `${this.config.apiUrl}/${task.id}`;
    return this.http.put<Task>(urlWithId, task);
  }

  addTask(task: Task): Observable<Task> {
    return this.http.post<Task>(this.config.apiUrl, task);
  }
}