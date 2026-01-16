import {Routes} from "@angular/router";
import {SignupComponent} from "./signup.component";
import {SigninComponent} from "./signin.component";
import {LogoutComponent} from "./logout.component";

export const AUTH_ROUTES: Routes = [
    //In here, the redirectTo will append 'signup' to the url.
    {path: '', redirectTo: 'signup', pathMatch: 'full' },
    {path: 'signup', component: SignupComponent},
    {path: 'signin', component: SigninComponent},
    {path: 'logout', component: LogoutComponent},
];
