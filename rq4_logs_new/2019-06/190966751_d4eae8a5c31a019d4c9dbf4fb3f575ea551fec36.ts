import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { IonicModule } from '@ionic/angular';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { AnaestheticsPage } from './anaesthetics.page';
import { DrugDisplayComponent } from './drug-display/drug-display.component';
import { DrugCardComponent } from './drug-display/drug-card/drug-card.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild([
      {
        path: '',
        component: AnaestheticsPage
      }
    ])
  ],
  declarations: [AnaestheticsPage, DrugDisplayComponent, DrugCardComponent]
})
export class AnaestheticsPageModule {}