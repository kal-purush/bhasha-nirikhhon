import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';

import { MaterializeModule } from 'angular2-materialize';
import { ChartModule } from 'angular2-highcharts';

import { employeesRouting } from './employees.routing';
import { EmployeesComponent } from './employees.component';
import { SessionService } from '../session/session.service';



@NgModule({
  declarations: [
    EmployeesComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    MaterializeModule,
    ChartModule,
    employeesRouting
  ],
  providers: [SessionService], // declared here to share data between components
  bootstrap: [EmployeesComponent]
})

export class EmployeesModule { }