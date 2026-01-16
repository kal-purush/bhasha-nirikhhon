import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { DailyAffairsComponent } from './daily-affairs/daily-affairs.component';
import { DailyAffairsRoutingModule } from './daily-affairs-routing.module';
import { MenuMarkdownModule } from '../../fe-common/menu-markdown/menu-markdown.module';

@NgModule({
  imports: [
    CommonModule,
    MenuMarkdownModule,
    DailyAffairsRoutingModule
  ],
  declarations: [DailyAffairsComponent]
})
export class DailyAffairsModule { }