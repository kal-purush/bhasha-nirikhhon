import { LoginGuard } from './login.guard';
import { LoginService } from './login.service';
import { SharedModule } from './../shared/shared.module';
import { SignUpComponent } from './sign-up.component';
import { SignInComponent } from './sign-in.component';

import {FormsModule} from "@angular/forms"
import {BrowserModule} from "@angular/platform-browser";
// Refer NgModule member from the @angular/core library
import {NgModule} from "@angular/core";
import { SinoutComponent } from './sinout.component'

// Decorate the class with Decorator NgModule

@NgModule({
    //Add components here i.e. registering the component
    declarations:[SignInComponent,SignUpComponent, SinoutComponent],
    //Startup component (root component details here)
    exports: [SignInComponent,SignUpComponent,SinoutComponent],
    // specify here the dependency modules
    imports:[SharedModule],
    providers:[LoginService,LoginGuard]

    
})
// Define a Class for the module
export class LoginModule {

}