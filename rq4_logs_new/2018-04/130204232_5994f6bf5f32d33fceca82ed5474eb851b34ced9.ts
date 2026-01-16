import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { SelectParamsPage } from './select-params';

@NgModule({
  declarations: [
    SelectParamsPage,
  ],
  imports: [
    IonicPageModule.forChild(SelectParamsPage),
  ],
})
export class SelectParamsPageModule {}