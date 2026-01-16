import { Component, NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { BaseDeDadosComponent } from './base-de-dados/base-de-dados.component';
import { PageNotFoundComponent } from './misc/page-not-found/page-not-found.component';
import { CalendarioComponent } from './calendario/calendario.component';
import { ConfiguracoesComponent } from './configuracoes/configuracoes.component';
import { VisaoGeralComponent } from './base-de-dados/tabelas/visao-geral/visao-geral.component';
import { NovoCadastroComponent } from './base-de-dados/novo-cadastro/novo-cadastro.component';
import { ErrorComponent } from './error/error/error.component';
import { FichaComponent } from './base-de-dados/ficha/ficha.component';
import { CadastroComponent } from './base-de-dados/ficha/abas-de-demandas/cadastro/cadastro.component';
import { FichaDemandaComponent } from './base-de-dados/ficha/abas-de-demandas/ficha-demanda/ficha-demanda.component';
import { AnexosComponent } from './base-de-dados/ficha/abas-de-demandas/anexos/anexos.component';
import { TabelaDemandaComponent } from './base-de-dados/tabelas/tabela-demanda/tabela-demanda.component';
import { EditarCadastroComponent } from './base-de-dados/ficha/editar-cadastro/editar-cadastro.component';

const routes: Routes = [
  { path: 'base-de-dados', component: BaseDeDadosComponent , children: [
    { path:'novo-cadastro', component: NovoCadastroComponent },
    {path: 'visao-geral', component: VisaoGeralComponent},
    {path: 'psicologia', component: TabelaDemandaComponent},
    {path: 'juridico', component: TabelaDemandaComponent},
    {path: 'servico-social', component: TabelaDemandaComponent},
    {path: 'abrigamentos', component: TabelaDemandaComponent},
    {path: '', redirectTo: 'visao-geral', pathMatch: 'full'}
  ]},

  {path: 'base-de-dados/ficha', component: FichaComponent},

  {path: 'base-de-dados/ficha/editar', component: EditarCadastroComponent},
  
  { path: 'calendario', component: CalendarioComponent },
  { path: 'configuracoes', component: ConfiguracoesComponent },

  { path: '', redirectTo: 'base-de-dados', pathMatch: 'full' }, // default route
  {path: 'error', component: ErrorComponent},
  { path: '**', component: PageNotFoundComponent }, // wildcard route for 404 not found
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }