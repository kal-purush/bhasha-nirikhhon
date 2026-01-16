import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from "@angular/router";

import { IonicModule } from '@ionic/angular';

import { UploadStudentVideoPageRoutingModule } from './upload-student-video-routing.module';

import { SharedComponentsModule } from 'components/shared-components/shared-components.module';
import { UploadStudentVideoPage } from './upload-student-video.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    SharedComponentsModule,
    UploadStudentVideoPageRoutingModule
  ],
  declarations: [UploadStudentVideoPage]
})
export class UploadStudentVideoPageModule {}