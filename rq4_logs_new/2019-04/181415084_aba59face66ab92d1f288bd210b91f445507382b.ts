import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { ContactPageComponent } from './components/contact-page/contact-page.component';
import { InfoPageComponent } from './components/info-page/info-page.component';
import { MainPageComponent } from './components/main-page/main-page.component';

const routs:Routes = [
  {
    path: '', component: MainPageComponent
  },
  {
    path: 'contact', component: ContactPageComponent
  },
  {
    path: 'info', component: InfoPageComponent
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routs)],
  exports: [RouterModule]
})
export class AppRoutingModule { }