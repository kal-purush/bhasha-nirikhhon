import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { SearchCityPageRoutingModule } from './search-city-routing.module';

import { SearchCityPage } from './search-city.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    SearchCityPageRoutingModule
  ],
  declarations: [SearchCityPage]
})
export class SearchCityPageModule {}