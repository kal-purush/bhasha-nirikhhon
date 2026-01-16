import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { MaterialModule } from '@angular/material';

import { AppComponent } from './app.component';
import { ViewerComponent } from "./viewer/viewer.component";
import { SchemaListComponent } from "./viewer/shared/schema-list.component";
import { SchemaItemComponent } from "./viewer/shared/schema-item.component";
import { Formatter } from "./shared/schema/formatters/v3.formatter";
import { V3Parser } from "./shared/schema/parsers/v3.parser";

@NgModule({
  declarations: [
    AppComponent,
    ViewerComponent,
    SchemaListComponent,
    SchemaItemComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    MaterialModule.forRoot()
  ],
  providers: [
    Formatter,
    V3Parser
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }