import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { OrganizationDetailsPageRoutingModule } from './organization-details-routing.module';

import { OrganizationDetailsPage } from './organization-details.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    OrganizationDetailsPageRoutingModule
  ],
  declarations: [OrganizationDetailsPage]
})
export class OrganizationDetailsPageModule {}