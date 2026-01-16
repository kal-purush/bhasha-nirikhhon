import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { UserRatingsPageRoutingModule } from './user-ratings-routing.module';

import { UserRatingsPage } from './user-ratings.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    UserRatingsPageRoutingModule
  ],
  declarations: [UserRatingsPage]
})
export class UserRatingsPageModule {}