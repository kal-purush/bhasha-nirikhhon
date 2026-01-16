import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from "@angular/platform-browser/animations"; //NUEVO
import { NoopAnimationsModule } from "@angular/platform-browser/animations"; //NUEVO: SIN ANIMACIONES
/*import { 
  MatCheckboxModule,
  MatRadioModule,
  MatSelectModule,
  MatInputModule
} from "@angular/material";*/ //NUEVO
import { NgModule } from '@angular/core';
import { HttpModule } from "@angular/http";
import { AppComponent } from './app.component';
import { TestComponent } from './test/test.component';
import { FormsModule } from '@angular/forms';
import 'hammerjs';
import { MaterialDesignModule } from './material-design/material-design.module';



@NgModule({
  declarations: [
    AppComponent,
    TestComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule, //NUEVO
    //NoopAnimationsModule //NUEVO
    /*MatCheckboxModule, //NUEVO
    MatRadioModule, //NUEVO
    MatSelectModule, //NUEVO
    MatInputModule,*/ //NUEVO: para control de formulario
    FormsModule, //NUEVO: para los objetos tipo input
    HttpModule,
    MaterialDesignModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }