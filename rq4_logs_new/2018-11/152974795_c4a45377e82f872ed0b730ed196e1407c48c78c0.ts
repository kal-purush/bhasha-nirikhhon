import {NgModule} from '@angular/core';
import {RouterModule, Routes} from "@angular/router";
import {DashboardComponent} from "./components/dashboard/dashboard.component";
import {AuthGuardService} from "../shared/auth/auth-guard.service";

const routes: Routes = [
  {
    path: '',
    // canActivateChild: [AuthGuardService],
    children: [
      {path: '', component: DashboardComponent}
    ]
  }
];


@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DashboardRoutingModule {
}