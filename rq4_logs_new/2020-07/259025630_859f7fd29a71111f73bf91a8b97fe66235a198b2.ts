import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { MobiscrollPageRoutingModule } from './mobiscroll-routing.module';

import { MobiscrollPage } from './mobiscroll.page';
import { MbscModule } from '@mobiscroll/angular';
import { HttpClientModule, HttpClientJsonpModule } from '@angular/common/http';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    MobiscrollPageRoutingModule,
    MbscModule,
    ReactiveFormsModule,
    HttpClientModule,
    HttpClientJsonpModule
  ],
  declarations: [MobiscrollPage]
})
export class MobiscrollPageModule {}