import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule }   from '@angular/forms';
import { AppComponent }   from './app.component';
import { NotaListComponent }  from './ejercicio/nota-list.component';
import { NotaEditComponent }  from './ejercicio/nota-edit.component';
import { NotaDetailComponent }  from './ejercicio/nota-detail.component';
import { RouterModule, Routes, Router } from '@angular/router';
import { AppRouterModule } from './ejercicio/approuter';

@NgModule({
  imports:      [ BrowserModule, FormsModule, AppRouterModule ],
  declarations: [ AppComponent, NotaListComponent, NotaDetailComponent, NotaEditComponent],
  bootstrap:    [ AppComponent, NotaListComponent ]
})
export class AppModule { }