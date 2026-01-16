import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { DirectorsComponent } from './directors/directors.component';

@NgModule({
  imports: [
    CommonModule
  ],
  exports:[DirectorsComponent],
  declarations: [DirectorsComponent]
})
export class DirectorsModule { }