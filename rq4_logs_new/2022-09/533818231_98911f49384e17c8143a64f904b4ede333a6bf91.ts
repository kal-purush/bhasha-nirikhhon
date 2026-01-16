import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AboutComponent } from './components/about/about.component';
import { HomeComponent } from './components/home/home.component';

const routes: Routes = [

{path:'', component:HomeComponent},
{path:'about', component:AboutComponent},

{ path: 'auth', loadChildren: () => import('./auth/auth.module').then(m => m.AuthModule) },

{ path: 'parcels', loadChildren: () => import('./users/users.module').then(m => m.UsersModule) },





];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }