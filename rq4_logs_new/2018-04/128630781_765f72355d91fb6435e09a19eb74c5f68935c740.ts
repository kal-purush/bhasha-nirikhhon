import { NgModule }                from '@angular/core';
import { CommonModule }            from '@angular/common';
import { SharedModule }            from '../../app/shared/shared.module';
import { LocationSearchComponent } from './location-search-component/location-search.component';
import { PipesModule }             from '../../pipes/pipes.module';

@NgModule({
  declarations: [
    LocationSearchComponent
  ],
  imports: [
  	CommonModule,
  	SharedModule,
    PipesModule
  ],
  exports: [
    LocationSearchComponent
  ],
  entryComponents:[
  	LocationSearchComponent
  ]
})
export class LocationSearchModule {}