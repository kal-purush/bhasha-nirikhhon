import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { DashboardComponent } from './dashboard.component';
import { MaterialModule } from '@angular/material';
import {GoogleFitSuggestionsComponent} from './google-fit-suggestions/google-fit-suggestions.component';
import { TotalsComponent } from './totals/totals.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule
  ],
  declarations: [DashboardComponent, GoogleFitSuggestionsComponent, TotalsComponent]
})
export class DashboardUiModule { }