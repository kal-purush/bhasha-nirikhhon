import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { ChannelChatPageRoutingModule } from './channel-chat-routing.module';

import { ChannelChatPage } from './channel-chat.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    ChannelChatPageRoutingModule
  ],
  declarations: [ChannelChatPage]
})
export class ChannelChatPageModule {}