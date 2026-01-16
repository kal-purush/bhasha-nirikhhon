import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { ValidacioActivitatPageRoutingModule } from './validacio-activitat-routing.module';

import { ValidacioActivitatPage } from './validacio-activitat.page';
import { SharedComponentsModule } from 'components/shared-components/shared-components.module';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    ValidacioActivitatPageRoutingModule,
    SharedComponentsModule
  ],
  declarations: [ValidacioActivitatPage]
})
export class ValidacioActivitatPageModule {}