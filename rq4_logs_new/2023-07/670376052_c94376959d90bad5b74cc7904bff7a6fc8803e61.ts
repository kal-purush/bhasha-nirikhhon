import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router'; // CLI imports router
import { CardComponent } from './card/card.component';
import { CardDetailsComponent } from './card-details/card-details.component';

const routes: Routes = [
    { path: '', redirectTo: '/cards', pathMatch: 'full' },
    { path: 'cards', component: CardComponent },
    { path: 'detail/:id', component: CardDetailsComponent },
  ];
// configures NgModule imports and exports
@NgModule({
  imports: [RouterModule.forRoot(routes, {scrollPositionRestoration: 'enabled', anchorScrolling: 'enabled'}) ],
  exports: [RouterModule]
})
export class AppRoutingModule { }