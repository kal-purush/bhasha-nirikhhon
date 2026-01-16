import { NgModule } from '@angular/core';
import { TooltipModule } from 'ngx-bootstrap/tooltip';
import { RouterModule, Routes } from '@angular/router';
import { HomeComponent } from './view/home/home.component';
import { DetailsComponent } from './view/details/details.component';

const routes: Routes = [
  {
    path: '',
    children: [
      { path: '', redirectTo: '/home', pathMatch: 'full' },
      { path: 'home', component: HomeComponent },
    ],
    component: HomeComponent
  },

  {
    path: 'details',
    children: [
      
    ],
    component: DetailsComponent
  },
]

@NgModule({
  imports: [RouterModule.forRoot(routes),TooltipModule.forRoot()],
  exports: [RouterModule]
})
export class AppRoutingModule { }