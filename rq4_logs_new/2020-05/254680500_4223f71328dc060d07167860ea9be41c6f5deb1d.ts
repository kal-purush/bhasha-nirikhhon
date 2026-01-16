import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '../shared/shared.module';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { AnnouncementEditComponent } from './announcement-edit/announcement-edit.component';
import { CreatePrivilegedComponent } from './create-privileged/create-privileged.component';
import { PrivilegedProfileComponent } from './privileged-profile/privileged-profile.component';



@NgModule({
  declarations: [
    AnnouncementEditComponent,
    CreatePrivilegedComponent,
    PrivilegedProfileComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    NgxChartsModule
  ]
})
export class AdminPanelModule { }