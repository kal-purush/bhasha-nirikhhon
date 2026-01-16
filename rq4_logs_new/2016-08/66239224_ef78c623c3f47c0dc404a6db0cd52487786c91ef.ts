import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpModule } from '@angular/http';

import { AppComponent }  from './app.component';

import { DataTableDirectives } from 'ng2-data-table/datatable';

@NgModule({
  imports: [
      BrowserModule,
      HttpModule
],
  declarations: [ AppComponent, DataTableDirectives ],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }