/**
 * Created on 2017-02-03.
 * @author: Gman Park
 */

import {RouterModule} from '@angular/router';
import {MovieComponent} from './movie/movie.component';
import {PageNotFoundComponent} from './util/pagenotfound.component';

export const routingModule = RouterModule.forRoot([
  {path: '', redirectTo: 'movie', pathMatch: 'full'},
  {
    path: 'movie', component: MovieComponent,
    children: [
      // {path: 'player/:id', component: PlayerComponent}
    ]
  },
  {
    path: '**',
    component: PageNotFoundComponent
  }
]);