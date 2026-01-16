import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { WorkspaceDetailsPageRoutingModule } from './workspace-details-routing.module';

import { WorkspaceDetailsPage } from './workspace-details.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    WorkspaceDetailsPageRoutingModule
  ],
  declarations: [WorkspaceDetailsPage]
})
export class WorkspaceDetailsPageModule {}