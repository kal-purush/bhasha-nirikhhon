import { Component, OnInit } from '@angular/core';
import {AuthService} from '../auth.service';
import {TokenModel, VisitModel} from '../app.component';
import {first} from 'rxjs/internal/operators';

@Component({
  selector: 'app-employee-employees-visits',
  templateUrl: './employee-employees-visits.component.html',
  styleUrls: ['./employee-employees-visits.component.css']
})
export class EmployeeEmployeesVisitsComponent implements OnInit {

    visits: VisitModel[] = [];

    constructor(private connection: AuthService) { }

    ngOnInit() {
        const token: TokenModel = {
            accessToken: this.connection.getAccessToken()
        };

        this.connection.getEmployeesVisits(token)
            .pipe(first())
            .subscribe(
                data => {
                    this.visits = data.visits;
                    for (let i = 0; i < data.visits.length; i++) {
                        const date = new Date(data.visits[i].visitDate);
                        data.visits[i].visitDate = date.getDate() + '-' + date.getMonth() + '-' + date.getFullYear();
                    }
                },
                error => {
                    console.log(error);
                }
            );
    }


}