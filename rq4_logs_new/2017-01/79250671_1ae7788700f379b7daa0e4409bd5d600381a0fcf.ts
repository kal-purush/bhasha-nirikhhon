/**
 * Created by natha on 21.01.2017.
 */
import {NgModule} from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import {BeamerComponent} from './components/beamer/beamer.component';
import {DashboardComponent} from './components/dashboard/dashboard.component';

const routes: Routes = [
    { path: '', redirectTo: '/dashboard', pathMatch: 'full' },
    { path: 'dashboard', component: DashboardComponent },
    { path: 'beamer', component: BeamerComponent }
];

@NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule]
})

export class AppRoutingModule {}