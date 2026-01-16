import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { WorkspaceDefaultPageRoutingModule } from './workspace-default-routing.module';

import { WorkspaceDefaultPage } from './workspace-default.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    WorkspaceDefaultPageRoutingModule
  ],
  declarations: [WorkspaceDefaultPage]
})
export class WorkspaceDefaultPageModule { }