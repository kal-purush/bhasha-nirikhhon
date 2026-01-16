import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { RunningLevelPageRoutingModule } from './running-level-routing.module';

import { RunningLevelPage } from './running-level.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RunningLevelPageRoutingModule
  ],
  declarations: [RunningLevelPage]
})
export class RunningLevelPageModule {}