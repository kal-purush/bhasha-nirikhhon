import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { loginRoutes } from './auth/auth-routing.module';
import { AuthGuard } from './auth/auth.guard';
import { EditarCidadeComponent } from './cidade/editar-cidade/editar-cidade.component';
import { InserirCidadeComponent } from './cidade/inserir-cidade/inserir-cidade.component';
import { ListarCidadeComponent } from './cidade/listar-cidade/listar-cidade.component';
import { EditarEnderecoComponent } from './endereco/editar-endereco/editar-endereco.component';
import { InserirEnderecoComponent } from './endereco/inserir-endereco/inserir-endereco.component';
import { ListarEnderecoComponent } from './endereco/listar-endereco/listar-endereco.component';
import { EditarEstadoComponent } from './estado/editar-estado/editar-estado.component';
import { InserirEstadoComponent } from './estado/inserir-estado/inserir-estado.component';
import { ListarEstadoComponent } from './estado/listar-estado/listar-estado.component';
import { HomeComponent } from './home/home.component';
import { EditarPessoaComponent } from './pessoa/editar-pessoa/editar-pessoa.component';
import { InserirPessoaComponent } from './pessoa/inserir-pessoa/inserir-pessoa.component';
import { ListarPessoaComponent } from './pessoa/listar-pessoa/listar-pessoa.component';

const routes: Routes = [
  {
    path: '',
    redirectTo: 'login',
    pathMatch: 'full',
  },

  {
    path: 'home',
    component: HomeComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE,FUNC' },
  },

  {
    path: 'pessoas',
    redirectTo: 'pessoas/listar',
  },
  {
    path: 'pessoas/listar',
    component: ListarPessoaComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE,FUNC' },
  },
  {
    path: 'pessoas/novo',
    component: InserirPessoaComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE,FUNC' },
  },
  {
    path: 'pessoas/editar/:id',
    component: EditarPessoaComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE,FUNC' },
  },

  {
    path: 'estados',
    redirectTo: 'estados/listar',
  },
  {
    path: 'estados/listar',
    component: ListarEstadoComponent,
    canActivate: [AuthGuard],
    data: { role: 'GERENTE' },
  },
  {
    path: 'estados/novo',
    component: InserirEstadoComponent,
    canActivate: [AuthGuard],
    data: { role: 'GERENTE' },
  },
  {
    path: 'estados/editar/:id',
    component: EditarEstadoComponent,
    canActivate: [AuthGuard],
    data: { role: 'GERENTE' },
  },

  {
    path: 'cidades',
    redirectTo: 'cidades/listar',
  },
  {
    path: 'cidades/listar',
    component: ListarCidadeComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,FUNC' },
  },
  {
    path: 'cidades/novo',
    component: InserirCidadeComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,FUNC' },
  },
  {
    path: 'cidades/editar/:id',
    component: EditarCidadeComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,FUNC' },
  },

  {
    path: 'enderecos',
    redirectTo: 'enderecos/listar',
  },
  {
    path: 'enderecos/listar',
    component: ListarEnderecoComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE' },
  },
  {
    path: 'enderecos/novo',
    component: InserirEnderecoComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE' },
  },
  {
    path: 'enderecos/editar/:id',
    component: EditarEnderecoComponent,
    canActivate: [AuthGuard],
    data: { role: 'ADMIN,GERENTE' },
  },

  ...loginRoutes,
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}