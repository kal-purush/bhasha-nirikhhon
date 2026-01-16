import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';
import { UIRulesComponent } from './ui-rules/ui-rules.component';
const routes: Routes = [
  {
    path: '',
    component: UIRulesComponent
  }
];

@NgModule({
  imports: [
    CommonModule,
    RouterModule.forChild(routes)
  ],
  exports: [RouterModule]
})
export class UIRulesRoutingModule { }