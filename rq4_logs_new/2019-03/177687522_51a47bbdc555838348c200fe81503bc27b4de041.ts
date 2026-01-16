import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Employee } from '@app/example/model/employee';
import { OauthToken } from '@app/shared/model/oauthToken';
import { UserTokenService, ClientType } from '@app/shared/services/user-token/user-token.service';
import { Observable } from 'rxjs';
import { flatMap } from 'rxjs/operators';
import { EmployeeService } from '../employee.service';

@Component({
  selector: 'app-employee-list',
  templateUrl: './employee-list.component.html',
  styleUrls: ['./employee-list.component.scss']
})
export class EmployeeListComponent implements OnInit {
  public mockFisrt = new Employee('-1', 'mock first');
  public employees = new Array<Employee>();
  private resourceUrl = '/employee/';
  loading = true;

  oauthToken: OauthToken;

  constructor(private employeeService: EmployeeService) { }

  ngOnInit() {
    const userToken = this.employeeService.getUserToken(ClientType.SPRING_CLOUND_EMPLOYEE_SERVICE);
    userToken.subscribe((oauthToken) => {
      window.sessionStorage.setItem('token', JSON.stringify(oauthToken));
      this.employeeService.getEmployeeList(this.resourceUrl).subscribe((employees) => {
        this.employees = employees;
        this.mockFisrt = this.employees[0];
        this.loading = false;
      });
    });

  }

}