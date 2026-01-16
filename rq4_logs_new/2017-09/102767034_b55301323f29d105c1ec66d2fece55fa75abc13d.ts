import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AuthGuardService } from 'app/core.module/services/auth-guard.service';
const routes: Routes = [
    { path: 'login-page', loadChildren: './login.module/login.module#LoginModule' },
    {
        path: '',
        loadChildren: './main.module/main.module#MainModule',
        canActivate: [AuthGuardService],
        data: {
            menuItem: 'Main',
            title: 'Enceladus'
        }
    },
    { path: '', redirectTo: '/login-page', pathMatch: 'full' },
    { path: '**', redirectTo: '/login-page', pathMatch: 'full' },
];

@NgModule({
    imports: [RouterModule.forRoot(routes, {
        // useHash: true,
        // enableTracing: true, // <-- debugging purposes only
    })],
    exports: [RouterModule],
    providers: [AuthGuardService]
})
export class AppRoutingModule { }
