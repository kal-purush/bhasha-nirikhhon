import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { NewTaskPage } from './new-task';

@NgModule({
  declarations: [
    NewTaskPage,
  ],
  imports: [
    IonicPageModule.forChild(NewTaskPage),
  ],
})
export class NewTaskPageModule {}