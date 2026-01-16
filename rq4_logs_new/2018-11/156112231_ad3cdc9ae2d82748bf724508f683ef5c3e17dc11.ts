import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { CurrentMapPage } from './current-map';

@NgModule({
  declarations: [
    CurrentMapPage,
  ],
  imports: [
    IonicPageModule.forChild(CurrentMapPage),
  ],
})
export class CurrentMapPageModule {}