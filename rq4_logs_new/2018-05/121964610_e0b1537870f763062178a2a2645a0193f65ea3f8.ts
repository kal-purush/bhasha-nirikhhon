import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { ShopitemPage } from './shopitem';

@NgModule({
  declarations: [
    ShopitemPage,
  ],
  imports: [
    IonicPageModule.forChild(ShopitemPage),
  ],
})
export class ShopitemPageModule {}