import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReusableComponentsModule } from "../../../survey-tool/app/shared/reusablecomponents/reusable-components.module";
import { HighchartsChartModule } from "highcharts-angular";
import { DataService } from "../services/data.service";
import { DataHandlerService } from "../services/data-handler.service";
import { StakeholdersService } from "../../../survey-tool/app/services/stakeholders.service";
import { EoscReadinessDataService } from "../services/eosc-readiness-data.service";
import { ExploreComponent } from "./explore.component";
import { ExploreRouting } from "./explore.routing";
import { OpenScienceByAreaComponent } from "./open-science-by-area/open-science-by-area.component";
import {CustomSearchComponent} from "./custom-search/custom-search.component";
import {InvestmentsInEoscComponent} from "./investments-in-eosc/investments-in-eosc.component";
import {NationalMonitoringComponent} from "./national-monitoring/national-monitoring.component";
import {OpenScienceByCountryComponent} from "./open-science-by-country/open-science-by-country.component";
import {OpenScienceImpactComponent} from "./open-science-impact/open-science-impact.component";
import {OpenSciencePoliciesComponent} from "./open-science-policies/open-science-policies.component";
import {
  OpenScienceResourceRegistryComponent
} from "./open-science-resource-registry/open-science-resource-registry.component";
import {OpenScienceTrendsComponent} from "./open-science-trends/open-science-trends.component";
import {OpenScienceUseCasesComponent} from "./open-science-use-cases/open-science-use-cases.component";

@NgModule({
  declarations: [
    ExploreComponent,
    OpenScienceByAreaComponent,
    CustomSearchComponent,
    InvestmentsInEoscComponent,
    NationalMonitoringComponent,
    OpenScienceByCountryComponent,
    OpenScienceImpactComponent,
    OpenSciencePoliciesComponent,
    OpenScienceResourceRegistryComponent,
    OpenScienceTrendsComponent,
    OpenScienceUseCasesComponent
  ],
  imports: [
    CommonModule,
    ReusableComponentsModule,
    ExploreRouting,
    HighchartsChartModule,
  ],
  providers: [
    DataService,
    EoscReadinessDataService,
    DataHandlerService,
    StakeholdersService
  ],
})

export class ExploreModule {}