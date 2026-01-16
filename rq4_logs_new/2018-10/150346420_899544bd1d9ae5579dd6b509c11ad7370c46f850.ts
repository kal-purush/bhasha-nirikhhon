import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { ModalFilterPage } from './modal-filter';

@NgModule({
  declarations: [
    ModalFilterPage,
  ],
  imports: [
    IonicPageModule.forChild(ModalFilterPage),
  ],
})
export class ModalFilterPageModule {}