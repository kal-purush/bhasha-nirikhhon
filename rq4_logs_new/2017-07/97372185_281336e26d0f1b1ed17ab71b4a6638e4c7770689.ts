/**
 * Created by 1 on 7/16/2017.
 */
import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {DemoComponent} from './test/demo-cmp';
import {NgSemanticModule} from 'ng-semantic';

@NgModule({
  imports: [
    CommonModule,
    NgSemanticModule
  ],
  declarations: [
    DemoComponent
  ],
  exports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    DemoComponent
  ]
})
export class SharedModule {}