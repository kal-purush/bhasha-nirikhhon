import { NgModule }       from '@angular/core';
import { CommonModule }   from '@angular/common';

import {LandingPageRoutingModule} from './landing-page.routing.module'

import {MaterialModule} from '../angularmaterials.module'

import { DashboardComponent }    from '../screens/dashboard/dashboard.component';
import {TimeSheetComponent} from '../screens/time-sheet/time-sheet.component'

import {UserTimeWidgetComponent} from '../widgets/user-time-widget/user-time-widget.component';
import { ProjectViewerComponent } from '../widgets/project-viewer/project-viewer.component';

import { ChartsModule } from 'ng2-charts/ng2-charts';


@NgModule({
  imports: [
    CommonModule,
    MaterialModule,
    LandingPageRoutingModule,
    ChartsModule,
  ],
  declarations: [
    DashboardComponent,
    UserTimeWidgetComponent,
    ProjectViewerComponent,
    TimeSheetComponent
  ],
  providers: [ ]
})
export class LandingPageModule {}