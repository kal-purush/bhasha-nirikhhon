import { NgModule } from "@angular/core";
import { RouterModule, Routes } from '@angular/router';

const routes: Routes = [
  {
    path: '',
    loadChildren: () => import('./landing/landing.module').then(m => m.LandingModule)
 },
 {
   path: 'farm-produce',
   loadChildren: () => import('./farmProduce/farmproduce.module').then(m => m.FarmProduceModule)
 }
];

@NgModule({
  imports: [
    RouterModule.forRoot(routes, {
      scrollPositionRestoration: "enabled",
      initialNavigation: 'enabled'
    }),
  ],
  exports: [
    RouterModule
  ]
})
export class AppRoutingModule{}