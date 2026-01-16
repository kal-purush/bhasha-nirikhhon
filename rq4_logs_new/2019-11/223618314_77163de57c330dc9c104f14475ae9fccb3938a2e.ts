import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';
import { retry } from 'rxjs/operators';
import { Employee } from './employee';

@Injectable({
  providedIn: 'root'
})
export class EmployeeService {

  private baseUrl = environment.apiUrl + 'employee/';

  constructor(private http: HttpClient) { }

  getAllEmployee() : Observable<any> {
    return this.http.get(this.baseUrl + 'all')
    .pipe(
      retry(3)
    );
  }

  saveEmployee(employee : Employee) : Observable<Employee> {
    return this.http.post<Employee>(this.baseUrl + 'add', employee);
  }

}