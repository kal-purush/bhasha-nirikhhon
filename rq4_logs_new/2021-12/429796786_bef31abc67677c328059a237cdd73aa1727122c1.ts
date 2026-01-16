import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { InscriptionComponent } from './inscription/inscription.component';
import { RouterModule, Routes } from '@angular/router';

export const CompteRouteList: Routes = [
  {
    path: '**',
    redirectTo: 'inscription'
  },
  {
    path: 'inscription',
    component: InscriptionComponent
  }
]

@NgModule({
  declarations: [
    InscriptionComponent
  ],
  imports: [
    CommonModule,
    RouterModule.forChild(CompteRouteList)
  ]
})
export class CompteModule { }