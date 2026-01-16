import { Routes } from '@angular/router';
import { HomeComponent } from '../home/home.component';
import { LoginComponent } from '../security/login/login.component';

export const routes: Routes = [
    { path: 'login',loadChildren: 'app/security/security.module#SecurityModule' },
    { path: '', loadChildren: 'app/security/security.module#SecurityModule'},
    { path: 'login', loadChildren: 'app/security/security.module#SecurityModule'},
    { path: '', loadChildren: 'app/security/security.module#SecurityModule'},
    { path: 'home',  component: HomeComponent },
    { path: '', redirectTo: '/home', pathMatch: 'full' }
];