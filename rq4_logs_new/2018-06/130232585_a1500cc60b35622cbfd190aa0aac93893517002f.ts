import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { SlideGalleryPage } from './slide-gallery';
import { SharedModule } from '../../app/shared/shared.module';

@NgModule({
  declarations: [
    SlideGalleryPage,
  ],
  imports: [
    IonicPageModule.forChild(SlideGalleryPage),
    SharedModule,
  ],
})
export class SlideGalleryPageModule {}