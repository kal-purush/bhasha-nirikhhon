import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { UploadComponent } from './containers/upload/upload.component';
import { RoutingModule } from './routing.module';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatDialogModule } from '@angular/material/dialog';
import { MatListModule } from '@angular/material/list';
import { FlexLayoutModule } from '@angular/flex-layout';
import { DialogComponent } from './components/dialog/dialog.component';
import { UploadService } from './services/upload.service';

@NgModule({
  imports: [
    CommonModule,
    RoutingModule,
    MatButtonModule,
    MatDialogModule,
    MatListModule,
    FlexLayoutModule,
    MatProgressBarModule,
  ],
  declarations: [UploadComponent, DialogComponent],
  providers: [UploadService]
})
export class FrontendSamplesModule {}