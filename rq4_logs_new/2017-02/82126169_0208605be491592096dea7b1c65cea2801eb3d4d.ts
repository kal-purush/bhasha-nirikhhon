// polyfills, comment the following out for debugging purpose
import 'core-js/es6';
import 'core-js/es7/reflect';
import 'zone.js/dist/zone';

// The browser platform with a compiler
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { NgModule }       from '@angular/core';
import { BrowserModule }  from '@angular/platform-browser';
import { FormsModule, ReactiveFormsModule }    from "@angular/forms";

import { AppComponent }   from './app.component';

//noinspection TypeScriptCheckImport
import { NiwaApplicationMessagesModule } from 'niwa-application-messages';
import { Ng2UtilsModule } from 'ng2-utils';

@NgModule({
  imports: [BrowserModule, FormsModule, ReactiveFormsModule, Ng2UtilsModule, NiwaApplicationMessagesModule],
  declarations: [AppComponent],
  bootstrap: [ AppComponent ]
})
export class AppModule { }

// Compile and launch the module
platformBrowserDynamic().bootstrapModule(AppModule);
