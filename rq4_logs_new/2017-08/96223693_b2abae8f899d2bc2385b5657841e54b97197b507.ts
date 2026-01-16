import { NgModule } from '@angular/core';
import { IonicModule } from 'ionic-angular';
import { TimeCounterComponent } from './time-counter';

@NgModule({
  declarations: [
    TimeCounterComponent,
  ],
  imports: [
    IonicModule,
  ],
  exports: [
    TimeCounterComponent
  ]
})
export class TimeCounterComponentModule {}