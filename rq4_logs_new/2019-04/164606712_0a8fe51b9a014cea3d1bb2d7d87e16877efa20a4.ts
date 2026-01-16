import { NgModule } from '@angular/core';

import { StatisticsRoutingModule } from './statistics-routing.module';
import { StatisticsComponent } from './statistics.component';
import { MenuComponent } from './components/menu/menu.component';
import { SummaryAnalysisComponent } from './components/summary-analysis/summary-analysis.component';
import { DataExportComponent } from './components/data-export/data-export.component';
import { SharedModule } from 'src/app/shared/shared.module';

@NgModule({
  declarations: [StatisticsComponent, MenuComponent, SummaryAnalysisComponent, DataExportComponent],
  imports: [
    StatisticsRoutingModule,
    SharedModule,
  ]
})
export class StatisticsModule { }