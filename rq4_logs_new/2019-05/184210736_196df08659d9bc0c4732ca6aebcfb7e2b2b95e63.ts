import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';

import { NuevosUsuariosPage } from './nuevos-usuarios.page';

const routes: Routes = [
  {
    path: '',
    component: NuevosUsuariosPage
  }
];

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes)
  ],
  declarations: [NuevosUsuariosPage]
})
export class NuevosUsuariosPageModule {}