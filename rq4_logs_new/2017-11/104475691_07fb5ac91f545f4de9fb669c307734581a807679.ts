import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { EditFoodPage } from './edit-food';

@NgModule({
  declarations: [
    EditFoodPage,
  ],
  imports: [
    IonicPageModule.forChild(EditFoodPage),
  ],
})
export class EditFoodPageModule {}