import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ListeFilmsComponent } from './liste-films/liste-films.component';
import { RouterModule, Routes } from '@angular/router';

export const FilmsRouteList: Routes = [
  { path: '', redirectTo: 'listeFilms', pathMatch: 'full' },
  { path: 'listeFilms', component: ListeFilmsComponent }
]

@NgModule({
  declarations: [
    ListeFilmsComponent
  ],
  imports: [
    CommonModule,
    RouterModule.forChild(FilmsRouteList)
  ]
})
export class FilmModule { }