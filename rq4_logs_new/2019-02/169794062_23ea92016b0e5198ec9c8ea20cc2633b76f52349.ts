import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

// components
import { RedirectComponent } from './core/services/auth/redirect/redirect.component';
import { HomeComponent } from './modules/home/home.component';

const routes: Routes = [
	// { path: '', redirectTo: '/', pathMatch: 'full' },
	{ path: 'redirect', component: RedirectComponent },
	{ path: 'redirect/token', component: RedirectComponent },
	{ path: 'home', component: HomeComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }