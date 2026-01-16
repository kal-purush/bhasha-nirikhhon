import { NgModule } from '@angular/core';
import {RouterModule,Routes} from '@angular/router';
import { ActorsListComponent } from './actors-list/actors-list.component';
import { AppComponent } from './app.component';
import { ActorsComponent } from './actors/actors.component';
import { UserComponent } from './user/user.component';
import { MoviesListComponent } from './movies-list/movies-list.component';
import { MoviesComponent } from './movies/movies.component';


const appRoutes: Routes = [
  {
    path: '',
    component : MoviesComponent
  },
  {
    path: 'actors',
    component: ActorsComponent
  },
  {
    path: 'movies',
    component: MoviesComponent
  },
  {
    path: 'moviesList',
    component: MoviesListComponent
  },
  {
    path: 'user',
    component: UserComponent 
  },
  {
    path: 'actorsList',
    component: ActorsListComponent
  }
];

@NgModule({
  imports: [RouterModule.forRoot(appRoutes)],
  exports: [RouterModule],
  declarations: [],
})
export class AppRoutingModule { }
export const routingComponents = [AppComponent,
  ActorsComponent , 
  MoviesComponent,
  MoviesListComponent,
  UserComponent ,
  ActorsListComponent]