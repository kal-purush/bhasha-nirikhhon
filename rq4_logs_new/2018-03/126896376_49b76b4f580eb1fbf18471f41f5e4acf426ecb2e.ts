import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { AppComponent } from './app.component';
import { MapComponent } from './map/map.component';
import { PageNotFoundComponent } from './page-not-found/page-not-found.component';
import { DisplayComponent } from './display/display.component';

const routes: Routes = [
    { path: 'map', component: MapComponent },
    { path: 'display', component: DisplayComponent },
    { path: '', pathMatch: 'full', component: DisplayComponent },
    { path: '**', component: PageNotFoundComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }