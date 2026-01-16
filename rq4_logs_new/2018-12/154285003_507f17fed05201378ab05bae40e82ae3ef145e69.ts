import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { AccelerometerPage } from './accelerometer';

@NgModule({
  declarations: [
    AccelerometerPage,
  ],
  imports: [
    IonicPageModule.forChild(AccelerometerPage),
  ],
})
export class AccelerometerPageModule {}