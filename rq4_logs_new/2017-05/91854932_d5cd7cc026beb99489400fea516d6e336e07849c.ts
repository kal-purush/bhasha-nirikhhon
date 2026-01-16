import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { PlayersPage } from './players';

@NgModule({
  declarations: [
    PlayersPage,
  ],
  imports: [
    IonicPageModule.forChild(PlayersPage),
  ],
  exports: [
    PlayersPage
  ]
})
export class PlayersPageModule {}