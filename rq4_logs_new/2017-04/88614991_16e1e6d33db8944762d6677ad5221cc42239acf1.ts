import { NgModule } from '@angular/core';
import { UniversalModule } from 'angular2-universal';
import { FormsModule } from '@angular/forms';


import { AppComponent } from './index';
import { RegisterComponent, LoginComponent } from './components/index';

@NgModule({
    /** Root App Component */
    bootstrap: [AppComponent],
    /** Our Components */
    declarations: [
        AppComponent,
        RegisterComponent,
        LoginComponent
    ],
    imports: [
        FormsModule,
        UniversalModule
    ],
    providers: []
})
export class AppModule {

}