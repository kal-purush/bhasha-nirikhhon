import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { BattlesComponent } from './components/battles/battles.component';
import { CaveComponent } from './components/cave/cave.component';
import { ForestComponent } from './components/forest/forest.component';
import { LoginComponent } from './components/login/login.component';

const routes: Routes = [

  {
    path:"",
    component:LoginComponent
  },

  {
    path:"login",
    component:LoginComponent
  },

  {
    path:"forest",
    component:ForestComponent
  },

  {
    path:"cave",
    component:CaveComponent
  },

  {
    path:"battles",
    component:BattlesComponent
  }

];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }