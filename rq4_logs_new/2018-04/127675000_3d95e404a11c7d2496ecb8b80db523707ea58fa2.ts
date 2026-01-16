import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NgstarterComponent } from './ngstarter/ngstarter.component';
import { MaterialAppModule } from '@app/ngmat/ngmat.module';
@NgModule({
  imports: [
    CommonModule,
    MaterialAppModule
  ],
  exports: [
    NgstarterComponent // We export the component so it can be used by importing this module
  ],
  declarations: [NgstarterComponent]
})
export class DemoModule { }