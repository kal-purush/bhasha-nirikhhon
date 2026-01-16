import { Component, OnInit } from '@angular/core';
import {AuthService} from '../auth.service';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {AddVisitModel, CarModel} from '../app.component';

@Component({
  selector: 'app-employee-add-visit',
  templateUrl: './employee-add-visit.component.html',
  styleUrls: ['./employee-add-visit.component.css']
})
export class EmployeeAddVisitComponent implements OnInit {

    carId: number;
    addVisitForm: FormGroup;
    cars: CarModel[] = [];

    constructor(
        private connection: AuthService,
        private formBuilder: FormBuilder
    ) { }

    ngOnInit() {

        this.addVisitForm = this.formBuilder.group({
            visitDate: ['', Validators.required],
            isOverview: ['', Validators.required],
        });
    }

    get f() { return this.addVisitForm.controls; }

    onSubmit() {

        if (this.addVisitForm.invalid) {
            return;
        }

        const date = new Date(this.f.visitDate.value);
        const visit: AddVisitModel = {
            accessToken: this.connection.getAccessToken(),
            carId: 0,
            visitDate: date.getDate() + '-' + (date.getMonth() + 1) + '-' + date.getFullYear(),
            isOverview: this.f.isOverview.value
        };

        this.connection.employeeAddVisit(visit);
    }

}