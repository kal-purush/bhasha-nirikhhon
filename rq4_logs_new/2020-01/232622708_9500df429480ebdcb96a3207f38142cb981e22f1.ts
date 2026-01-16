import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {UsuarioComponent} from './usuarios/usuario.component';
import {HomeComponent} from './pages/home/home.component';
import {RajidComponent} from './rajid/rajid.component';
import {Rajid42Component} from './rajid42/rajid42.component';


const routes: Routes = [
  {
    path: 'rajid42', component: Rajid42Component
  },
  {
    path: 'rajid', component: RajidComponent
  },
  {
    path: 'usuarios', component: UsuarioComponent
  },
  {
    path: '', component: HomeComponent
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }