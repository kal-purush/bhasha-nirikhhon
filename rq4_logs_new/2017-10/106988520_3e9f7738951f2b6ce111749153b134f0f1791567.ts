
import { NgModule        } from '@angular/core';
import { CommonModule    } from '@angular/common';

import {
  MATERIAL_COMPATIBILITY_MODE,
  MatButtonModule 
} from '@angular/material';

@NgModule({
  imports: [
    CommonModule,
    MatButtonModule
  ],
  exports: [
    MatButtonModule
  ],
  providers: [
    { provide: MATERIAL_COMPATIBILITY_MODE, useValue: true }
  ],
  declarations: []
})

export class MaterialModule { }