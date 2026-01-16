import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { DetailPlacePage } from './detail-place';

@NgModule({
  declarations: [
    DetailPlacePage,
  ],
  imports: [
    IonicPageModule.forChild(DetailPlacePage),
  ],
})
export class DetailPlacePageModule {}