import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from "@angular/router";

import { IonicModule } from '@ionic/angular';

import { UploadActivityVideoPageRoutingModule } from './upload-activity-video-routing.module';

import { SharedComponentsModule } from 'components/shared-components/shared-components.module';
import { UploadActivityVideoPage } from './upload-activity-video.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    SharedComponentsModule,
    UploadActivityVideoPageRoutingModule
  ],
  declarations: [UploadActivityVideoPage]
})
export class UploadActivityVideoPageModule {}