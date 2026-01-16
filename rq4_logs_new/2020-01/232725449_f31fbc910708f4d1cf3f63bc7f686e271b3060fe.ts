import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { FormsModule,ReactiveFormsModule} from '@angular/forms';


import{MaterialanModule} from './materialan/materialan.module';
import { LandPageComponent } from './land-page/land-page.component';
import { MatDatepickerModule, MatGridListModule, MatCardModule, MatInputModule, MatButtonModule } from '@angular/material';
import { CommonModule } from '@angular/common';
import { LoginComponent } from './login/login.component';
import { MatCarouselModule } from '@ngmodule/material-carousel';

@NgModule({
  declarations: [
    AppComponent,
    LandPageComponent,
    LoginComponent,
    
    
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,

    FormsModule,
    MaterialanModule,
    MatDatepickerModule,
    CommonModule,
    MatGridListModule,
    FormsModule,
    ReactiveFormsModule,
    MatCardModule,
    MatInputModule,
    MatButtonModule,
    MatCarouselModule
 
  
   ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }