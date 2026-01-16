import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {RouterModule, Routes} from '@angular/router';
import {PesquisaCategoriasComponent} from './pesquisa/pesquisa-categorias.component';
import {AuthGuard} from '../../security/auth.guard';

const routes: Routes = [
  {
    path: '',
    component: PesquisaCategoriasComponent,
    canActivate: [AuthGuard],
    data: {
      authorities: ['LISTAR_CATEGORIA']
    }
  },
  {
    path: '**',
    redirectTo: '',
    pathMatch: 'full'
  }
];

@NgModule({
  declarations: [],
  imports: [
    CommonModule,
    RouterModule.forChild(routes)
  ],
  exports: [RouterModule]
})
export class CategoriasRoutingModule {
}