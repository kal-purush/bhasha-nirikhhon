import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';
import { FichaListComponent } from './../fichas/ficha-list/ficha-list.component';
import { PaginaListarFichasPage } from './pagina-listar-fichas.page';
import { MatExpansionModule, MatButtonModule, MatPaginatorModule } from '@angular/material';

const routes: Routes = [
  {
    path: '',
    component: PaginaListarFichasPage
  }
];

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes),
    MatExpansionModule,
    MatButtonModule,
    MatPaginatorModule
  ],
  declarations: [PaginaListarFichasPage,  FichaListComponent]
})
export class PaginaListarFichasPageModule {}