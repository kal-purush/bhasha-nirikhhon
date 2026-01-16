import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';

import { AppComponent } from './app.component';
import { StudyPhaseComponent } from './studyphase.component';
import {RouterModule, Routes} from "@angular/router";
import {TestPhaseComponent} from "./testphase.component";
import {TestComponent} from "./test.component"

const appRoutes: Routes = [
  // { path: 'intro', component: IntroPhaseComponent },
  { path: 'study-phase', component: StudyPhaseComponent },
  { path: 'test-phase', component: TestPhaseComponent },
  { path: 'test', component: TestComponent}
]
@NgModule({
  declarations: [
    AppComponent,
    StudyPhaseComponent,
    TestPhaseComponent,
    TestComponent
  ],
  imports: [
    RouterModule.forRoot(appRoutes),
    BrowserModule,
    FormsModule,
    HttpModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }