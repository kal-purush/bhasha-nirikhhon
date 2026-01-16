import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import {PatientsComponent} from './patients/patients.component';
import {PatientDetailComponent} from './patient-detail/patient-detail.component';
import {NavigateComponent} from './navigate/navigate.component';


const routes: Routes = [
  {path: 'detail/id', component: PatientDetailComponent},
  {path: 'patients', component: PatientsComponent},
  {path:  'navigate', component: NavigateComponent},
  {path: '', redirectTo: '/navigate', pathMatch: 'full'}];
@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})


export class AppRoutingModule {
}