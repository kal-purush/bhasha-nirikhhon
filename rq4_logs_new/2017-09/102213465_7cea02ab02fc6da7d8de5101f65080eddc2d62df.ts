import { NgModule } from '@angular/core'
import { RouterModule, Routes } from '@angular/router'
import { EditionDetailComponent } from './edition-detail.component'
import { EditionListComponent } from "./edition-list.component";
import { HomeComponent } from "./home.component";

var routes: Routes = [
    { path: '', redirectTo: '/home', pathMatch: 'full' },
    { path: 'home', component: HomeComponent },
    { path: 'edicoes', component: EditionListComponent },
    { path: 'edicao/:id', component: EditionDetailComponent },
]

@NgModule({
    imports: [RouterModule.forRoot(routes, { useHash: true })],
    exports: [RouterModule]
})
export class AppRouterModule { }