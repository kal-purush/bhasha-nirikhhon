import { NgModule, Optional, SkipSelf } from '@angular/core';
import { CommonModule } from '@angular/common';
//Modules
import { MaterialModule } from './material.module';

//Components
import { NavbarComponent } from './navbar/navbar.component';
//Directives

//Pipes


@NgModule({
  declarations: [NavbarComponent],
  exports: [MaterialModule, NavbarComponent],
  imports: [
    CommonModule,
    MaterialModule
  ]
})
export class CoreModule {
  constructor (@Optional() @SkipSelf() parentModule: CoreModule) {
    if (parentModule) {
      throw new Error(
        'Core is already loaded. Import it in the AppModule only');
    }
  }
 }