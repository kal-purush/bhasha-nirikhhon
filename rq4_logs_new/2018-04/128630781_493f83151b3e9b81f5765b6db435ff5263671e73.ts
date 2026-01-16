import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '../../app/shared/shared.module';
import { ListfullComponent } from './listfull-component/listfull.component';
import { PipesModule } from '../../pipes/pipes.module';

@NgModule({
  declarations: [
    ListfullComponent
  ],
  imports: [
  	CommonModule,
  	SharedModule,
    PipesModule
  ],
  exports: [
    ListfullComponent
  ],
  entryComponents:[
  	ListfullComponent
  ]
})
export class ListfullModule {}