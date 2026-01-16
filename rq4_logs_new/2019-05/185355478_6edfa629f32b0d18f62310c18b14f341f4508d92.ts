import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';

import { PaginaBaseDadosPage } from './pagina-base-dados.page';
import { FichaListComponent } from './../fichas/ficha-list/ficha-list.component';
import { MatExpansionModule } from '@angular/material';

const routes: Routes = [
  {
    path: '',
    component: PaginaBaseDadosPage
  }
];

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes),
    MatExpansionModule,
  ],
  declarations: [PaginaBaseDadosPage, FichaListComponent]
})
export class PaginaBaseDadosPageModule {}