import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';

import { PaginaFichaAvaliacaoPage } from './pagina-ficha-avaliacao.page';
import { FichaCreateComponent } from './../fichas/ficha-create/ficha-create.component';

const routes: Routes = [
  {
    path: '',
    component: PaginaFichaAvaliacaoPage
  }
];

@NgModule({
  imports: [
  CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes)
  ],
  declarations: [
    PaginaFichaAvaliacaoPage,
    FichaCreateComponent
  ]
})
export class PaginaFichaAvaliacaoPageModule {}