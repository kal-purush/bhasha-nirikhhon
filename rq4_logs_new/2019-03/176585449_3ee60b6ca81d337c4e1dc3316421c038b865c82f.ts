import { NgModule } from '@angular/core';

import { GixButtonCornectoComponent } from './gix-button-cornecto/gix-button-cornecto.component';

import { MatIconModule } from '@angular/material/icon';

import { MatRippleModule } from '@angular/material/core';

import {MatButtonModule} from '@angular/material/button';
import { GixButtonTwolcornComponent } from './gix-button-twolcorn/gix-button-twolcorn.component';


@NgModule({
  declarations: [ GixButtonCornectoComponent, GixButtonTwolcornComponent],
  imports: [
    MatIconModule,
    MatRippleModule,
    MatButtonModule
  ],
  exports: [GixButtonCornectoComponent,GixButtonTwolcornComponent]
})
export class GIXNGButtonsModule { }