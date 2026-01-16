import { Injectable } from '@angular/core';
import { Student } from './Types/pollInstance';
import { environment } from '../../../environments/environment';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root',
})
export class StudentService {
  private apiUrl = environment.apiUrl;
  private endpoint = 'api/v1/Students';

  constructor(private http: HttpClient) {}

  update(data: Student) {
    console.log('update');
    console.log(data);
    return this.http.patch<Student>(`${this.apiUrl}/${this.endpoint}`, data);
  }

  remove(id: number) {
    return this.http.delete<Student>(`${this.apiUrl}/${this.endpoint}/${id}`);
  }
}