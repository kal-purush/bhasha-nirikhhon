import { Component, OnInit } from '@angular/core';
import { SessionService, User, Company } from '../session/session.service';
import { EmployeesService } from './employees.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-employees',
  templateUrl: './employees.component.html',
  styleUrls: ['./employees.component.css'],
  providers: [EmployeesService] // declared here as it only services this component
})
export class DashboardComponent implements OnInit {
  user: User;
  company: Company;
  data: any;

    constructor(private sessionService: SessionService, private router: Router, private employeesService: EmployeesService) {
      if (!this.sessionService.isLoggedIn()) {
        this.router.navigate(['/']);
      }

      this.user = this.sessionService.getUser();
      this.company = this.sessionService.getCompany();

      this.sessionService.companySwitch.subscribe(c => {
        this.company = c;
        this.setData();
      });
    }

    ngOnInit() {
      this.setData();
    }

    setData() {
      this.employeesService.index(this.company.id).subscribe(res => {
        this.data = res.json().data;
      });
    }
}