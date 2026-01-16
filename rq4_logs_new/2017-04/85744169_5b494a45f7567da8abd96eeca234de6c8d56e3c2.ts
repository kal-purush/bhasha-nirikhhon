
import {ModuleWithProviders} from '@angular/core';
import {Routes, RouterModule} from '@angular/router';

import { NgModule } from '@angular/core';


///Componentes///
import { AppComponent } from './app.component';
import { LoginComponent } from './Components/home/home.component';
import { AboutusComponent } from './Components/aboutus/aboutus.component';
import { ResetComponent } from './Components/reset/reset.component';
import { BarComponent } from './Components/bar/bar.component';
import { EmployeesComponent } from './Components/employees/employees.component';
import { NovetiesComponent } from './Components/noveties/noveties.component';
import { LiquidationsComponent } from './Components/liquidations/liquidations.component';
import { StatisticsComponent } from './Components/statistics/statistics.component';
import { ParametersComponent } from './Components/parameters/parameters.component';


const routes: Routes= [
    { path:'', component: LoginComponent },
    { path:'aboutus', component: AboutusComponent},
    { path:'reset', component: ResetComponent },
    { path:'employees', component: EmployeesComponent },
    { path:'noveties', component: NovetiesComponent },
    { path:'liquidations', component: LiquidationsComponent },
    { path:'statistics', component: StatisticsComponent },
    { path:'parameters', component: ParametersComponent }
];

@NgModule({
    imports : [ RouterModule.forRoot( routes ) ],
    exports : [ RouterModule]
})

export class AppRoutingModule {}

export const routingComponent = [    
    
    LoginComponent,
    AboutusComponent,
    ResetComponent,
    BarComponent,
    EmployeesComponent,
    NovetiesComponent,
    LiquidationsComponent,
    StatisticsComponent,
    ParametersComponent]