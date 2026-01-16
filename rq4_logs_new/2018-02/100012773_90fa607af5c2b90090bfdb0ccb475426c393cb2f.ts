import { FindGroupPage } from './findgroup-page';
import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';

@NgModule({
  declarations: [
    FindGroupPage,
  ],
  imports: [
    IonicPageModule.forChild(FindGroupPage),
  ],
  exports: [
    FindGroupPage
  ]
})

export class FindGroupPageModule { }