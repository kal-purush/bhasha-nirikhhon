import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import {FlexLayoutModule} from "@angular/flex-layout";
import {
  MatButtonModule,
  MatCardModule,
  MatIconModule,
  MatListModule,
  MatToolbarModule
} from "@angular/material";
import {NotFoundComponent} from "../shared/not-found/not-found.component";
import {RouterModule} from "@angular/router";

const uiModules = [
  CommonModule,
  FlexLayoutModule,
  MatIconModule,
  MatCardModule,
  MatButtonModule,
  MatListModule,
  MatToolbarModule
];

@NgModule({
  declarations: [NotFoundComponent],
  imports: [
    RouterModule,
    uiModules
  ],
  exports : [uiModules, NotFoundComponent]
})
export class MainSharedModule { }