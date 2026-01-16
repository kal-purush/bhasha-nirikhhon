import { Injectable } from '@angular/core';
import { HttpClient} from '@angular/common/http';
import { Observable } from 'rxjs';
import { STUDENTS_TABLE_URL } from '../../environments/environment';

export interface Student {
    name: string;
    email: string;
    technology: string;
    status: string;
    telephone: string;
    github: string;
    hrManager: string;
}

@Injectable({
  providedIn: 'root',
})
export class StudentsService {

  constructor(private http: HttpClient) {}

  fetchEvents(): Observable<Student[]> {
    return this.http.get<Student[]>(STUDENTS_TABLE_URL);
  }

  fetchStudentByEmail(email: string): Observable<Student> {
    return this.http.get<Student>(`${STUDENTS_TABLE_URL}/${email}`);
  }
}