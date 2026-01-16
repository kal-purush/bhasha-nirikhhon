import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TeamBuildingComponent } from './team-building/team-building.component';
import { TeamBuildingRoutingModule } from './team-building-routing.module';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { FooterModule } from '../../footer/footer.module';
import { MenuMarkdownModule } from '../../fe-common/menu-markdown/menu-markdown.module';

@NgModule({
  imports: [
    CommonModule,
    NgZorroAntdModule,
    FooterModule,
    MenuMarkdownModule,
    TeamBuildingRoutingModule
  ],
  declarations: [TeamBuildingComponent]
})
export class TeamBuildingModule { }