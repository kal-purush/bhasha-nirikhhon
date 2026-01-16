import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';
import { DashboardComponent } from '../components/dashboard/dashboard.component';
import { AboutComponent } from '../components/about/about.component';
import { PortfolioComponent } from '../components/portfolio/portfolio.component';
import { ContactusComponent } from '../components/contactus/contactus.component';
import { ServicesComponent } from '../components/services/services.component';

const routes: Routes = [
    {
      path: '',
      component: DashboardComponent,
    },
    {
      path: 'about',
      component: AboutComponent,
    },
    {
      path: 'portfolio',
      component: PortfolioComponent
    },
    {
      path: 'contactus',
      component: ContactusComponent
    },
    {
      path: 'services',
      component: ServicesComponent
    }
];
@NgModule({
  imports: [
    CommonModule,
    RouterModule.forRoot(routes)
  ],
  exports: [
    RouterModule
  ],
  declarations: []
})
export class AppRoutingModule { }