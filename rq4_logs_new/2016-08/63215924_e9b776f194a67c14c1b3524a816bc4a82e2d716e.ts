import { NgModule, provide }       from '@angular/core';
import { FormsModule } from '@angular/forms';
import { BrowserModule } from '@angular/platform-browser';
import { HttpModule } from '@angular/http';

import { SharedModule }   from './shared';
import { constAppSettings, appSettings } from './shared';

import { routing }        from './app.routing';
import { AppComponent }   from './app.component';

@NgModule({
    declarations: [AppComponent],
    imports:      [
        BrowserModule,
        routing,
        SharedModule.forRoot(),
        HttpModule,  // ??
    ],
    bootstrap:    [AppComponent],
    providers: [
        provide(appSettings, { useValue: constAppSettings }),
    ],
})
export class AppModule {}