import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { ViewMyActivityPage } from './view-my-activity';

@NgModule({
  declarations: [
    ViewMyActivityPage,
  ],
  imports: [
    IonicPageModule.forChild(ViewMyActivityPage),
  ],
  exports: [
    ViewMyActivityPage
  ]
})
export class ViewMyActivityPageModule {}