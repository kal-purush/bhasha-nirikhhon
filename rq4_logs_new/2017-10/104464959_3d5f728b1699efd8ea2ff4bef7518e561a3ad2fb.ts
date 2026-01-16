import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';

@Injectable()
export class TodoService {
  private apiUrl = 'http://59bf827da101d20011afd550.mockapi.io/api/Todo/';

  constructor(private http: Http) {}

  getData() {
    return this.http.get(this.apiUrl)
      .map(result => result.json());
  }

  getTodoById(id) {
    return this.http.get(this.apiUrl + id)
      .map(result => result.json());
  }

  update(id: number, todo: any) {
    return this.http.put(this.apiUrl + id, todo)
      .map(result => result.json());
  }

  add(todo: any) {
    return this.http.post(this.apiUrl, todo)
      .map(result => result.json());
  }

  delete(id: number) {
    return this.http.delete(this.apiUrl + id)
      .map(result => result.json());
  }
}