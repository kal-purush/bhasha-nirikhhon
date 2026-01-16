import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { AnnualFlowGraphsPageRoutingModule } from './annual-flow-graphs-routing.module';

import { AnnualFlowGraphsPage } from './annual-flow-graphs.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    AnnualFlowGraphsPageRoutingModule
  ],
  declarations: [AnnualFlowGraphsPage]
})
export class AnnualFlowGraphsPageModule {}