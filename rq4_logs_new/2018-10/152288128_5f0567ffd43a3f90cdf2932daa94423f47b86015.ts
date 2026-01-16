import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { CadastroComponent} from './cadastro/cadastro.component';
import { GerenciamentoComponent} from './gerenciamento/gerenciamento.component';

const routes: Routes = [
  { 
    path: '', 
  component: CadastroComponent
},
  { 
    path: 'Gerenciamento', 
    component: GerenciamentoComponent
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }