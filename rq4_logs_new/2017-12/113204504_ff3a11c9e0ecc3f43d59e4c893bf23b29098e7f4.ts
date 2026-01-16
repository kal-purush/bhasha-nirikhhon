import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AvatarModule } from '../../../../shared/module/avatar/avatar.module';
import { SharePipesModule } from '../../../../shared/pipes';
import { InfoTeamComponent } from './info-team.component';
import { PlayerItemModule } from '../player-item';

@NgModule({
  imports: [
    CommonModule,
    AvatarModule,
    SharePipesModule,
    PlayerItemModule
  ],
  declarations: [
    InfoTeamComponent
  ],
  exports: [
    InfoTeamComponent
  ]
})
export class InfoTeamModule {
}