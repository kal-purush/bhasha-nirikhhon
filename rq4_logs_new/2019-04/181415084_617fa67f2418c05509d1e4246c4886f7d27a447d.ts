import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

//components
import { InfoPageComponent } from './info-page.component';

//ngx-bootstrap
import { CollapseModule } from 'ngx-bootstrap/collapse';

@NgModule({
  imports: [
    CommonModule,
    CollapseModule.forRoot(),
  ],
  exports: [
    InfoPageComponent,
  ],
  declarations: [
    InfoPageComponent,
  ]
})
export class InfoPageModule { }