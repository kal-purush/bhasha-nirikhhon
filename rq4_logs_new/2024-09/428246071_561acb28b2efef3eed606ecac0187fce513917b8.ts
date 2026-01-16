import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import {OpenScienceByAreaComponent} from "./open-science-by-area.component";
import {
  ReusableComponentsModule
} from "../../../../survey-tool/app/shared/reusablecomponents/reusable-components.module";
import {OpenScienceByAreaRouting} from "./open-science-by-area.routing";
import {HighchartsChartModule} from "highcharts-angular";
import {DataService} from "../../services/data.service";
import {EoscReadinessDataService} from "../../services/eosc-readiness-data.service";
import {DataHandlerService} from "../../services/data-handler.service";
import {StakeholdersService} from "../../../../survey-tool/app/services/stakeholders.service";
import {OpenScienceByAreaPublicationsComponent} from "./areas/open-science-by-area-publications.component";
import {OpenScienceByAreaOpenDataComponent} from "./areas/open-science-by-area-open-data.component";

@NgModule({
  declarations: [
    OpenScienceByAreaComponent,
    OpenScienceByAreaPublicationsComponent,
    OpenScienceByAreaOpenDataComponent
  ],
  imports: [
    CommonModule,
    ReusableComponentsModule,
    OpenScienceByAreaRouting,
    HighchartsChartModule,
  ],
  providers: [
    DataService,
    EoscReadinessDataService,
    DataHandlerService,
    StakeholdersService
  ],
})

export class OpenScienceByAreaModule {}