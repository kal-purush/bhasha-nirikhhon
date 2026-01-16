import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { MatSidenavModule } from '@angular/material/sidenav';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatIconModule } from '@angular/material/icon';

const angularMaterialModules = [
  MatSidenavModule,
  MatToolbarModule,
  MatIconModule,
];

@NgModule({
  imports: [
    CommonModule,
    angularMaterialModules,
  ],
  exports: [angularMaterialModules]
})
export class AngularMaterialModule { }