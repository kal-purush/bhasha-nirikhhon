import { NgModule } from '@angular/core';
import { MatButtonModule, MatInputModule } from '@angular/material';

import { AppComponent } from './app.component';
import { PkInputSearchComponent } from './pk-input-search/pk-input-search.component';
import { BrowserAnimationsModule, NoopAnimationsModule } from '@angular/platform-browser/animations';

@NgModule({
  declarations: [
    PkInputSearchComponent
  ],
  exports: [
    PkInputSearchComponent,
    MatButtonModule,
    MatInputModule,
    BrowserAnimationsModule,
    NoopAnimationsModule
  ],
  imports: [
    MatButtonModule,
    MatInputModule,
    BrowserAnimationsModule,
    NoopAnimationsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppMaterialsModule { }