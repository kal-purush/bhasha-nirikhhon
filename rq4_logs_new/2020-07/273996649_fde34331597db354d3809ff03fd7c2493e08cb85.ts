import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { ChannelDefaultPageRoutingModule } from './channel-default-routing.module';

import { ChannelDefaultPage } from './channel-default.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    ChannelDefaultPageRoutingModule
  ],
  declarations: [ChannelDefaultPage]
})
export class ChannelDefaultPageModule {}