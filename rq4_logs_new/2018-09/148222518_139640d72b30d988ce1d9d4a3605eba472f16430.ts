import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpModule } from '@angular/http';
import { FormsModule } from '@angular/forms';

import { PlanetViewerComponent } from './container/planet-viewer.component';
import { HeaderComponent } from './components/header-component/header.component';

export const COMPONENTS = [
  PlanetViewerComponent,
  HeaderComponent

];

@NgModule({
  declarations: COMPONENTS,
  imports: [
    HttpModule,
    FormsModule
  ],
  exports: [
    PlanetViewerComponent
  ],
  providers: [
  ]
})
export class PlanetViewerModule { }