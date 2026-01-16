import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { WorkspaceListPageRoutingModule } from './workspace-list-routing.module';

import { WorkspaceListPage } from './workspace-list.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    WorkspaceListPageRoutingModule
  ],
  declarations: [WorkspaceListPage]
})
export class WorkspaceListPageModule {}