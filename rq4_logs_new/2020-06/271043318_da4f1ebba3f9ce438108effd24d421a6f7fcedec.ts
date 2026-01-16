import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { RatingCreatePageRoutingModule } from './rating-create-routing.module';

import { RatingCreatePage } from './rating-create.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RatingCreatePageRoutingModule
  ],
  declarations: [RatingCreatePage]
})
export class RatingCreatePageModule {}