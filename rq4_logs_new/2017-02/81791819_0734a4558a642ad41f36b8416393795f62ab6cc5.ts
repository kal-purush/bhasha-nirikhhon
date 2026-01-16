/**
 * Created by ahsanayaz on 14/02/2017.
 */


import { Routes, RouterModule } from '@angular/router';
import { LoginComponent } from './login/login2.component';
import { AuthGuard } from './auth/auth.guard';
import { PrivateComponent } from './private/private.component';

var appRoutes:any = [
    {
        path: 'dashboard',
        //component: DashboardComponent,
        component:  PrivateComponent,
        canActivate: [ AuthGuard ]
    },
    {
        path: 'login',
        component: LoginComponent
    },
    {
        path: '',
        redirectTo: 'dashboard',
        pathMatch: 'full'
    }
];

export const ROUTES: Routes = appRoutes;