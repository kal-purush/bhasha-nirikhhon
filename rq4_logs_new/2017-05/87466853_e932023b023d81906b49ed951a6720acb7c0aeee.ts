import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { ConfirmItem } from './confirm-item';

@NgModule({
  declarations: [
    ConfirmItem,
  ],
  imports: [
    IonicPageModule.forChild(ConfirmItem),
  ],
  exports: [
    ConfirmItem
  ]
})
export class ConfirmItemModule {}