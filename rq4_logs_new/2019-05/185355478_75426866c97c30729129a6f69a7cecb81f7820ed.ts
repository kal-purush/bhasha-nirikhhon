import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';

import { PaginaCadastroPage } from './pagina-cadastro.page';
import { CadastroComponent } from '../auth/cadastro/cadastro.component';
import { MatCardModule, MatInputModule, MatButtonModule, MatToolbarModule, MatDividerModule } from '@angular/material';

const routes: Routes = [
  {
    path: '',
    component: PaginaCadastroPage
  }
];

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes),
    MatCardModule,
    MatInputModule,
    MatButtonModule,
    MatToolbarModule,
    MatDividerModule,
  ],
  declarations: [PaginaCadastroPage, CadastroComponent]
})
export class PaginaCadastroPageModule {}