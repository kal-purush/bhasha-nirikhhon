import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { FilepondPageRoutingModule } from './filepond-routing.module';

import { FilepondPage } from './filepond.page';
// import filepond module
import { FilePondModule, registerPlugin } from 'ngx-filepond';
import FilePondPluginImagePreview from 'filepond-plugin-image-preview';
registerPlugin(FilePondPluginImagePreview);

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    FilepondPageRoutingModule,
    FilePondModule
  ],
  declarations: [FilepondPage]
})
export class FilepondPageModule {}