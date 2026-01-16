import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { EnrolPage } from './enrol';

@NgModule({
  declarations: [
    EnrolPage,
  ],
  imports: [
    IonicPageModule.forChild(EnrolPage),
  ],
})
export class EnrolPageModule {}