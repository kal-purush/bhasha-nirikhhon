import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { SloptionsPage } from './sloptions';

@NgModule({
  declarations: [
    SloptionsPage,
  ],
  imports: [
    IonicPageModule.forChild(SloptionsPage),
  ],
  exports: [
    SloptionsPage
  ]
})
export class SloptionsPageModule {}