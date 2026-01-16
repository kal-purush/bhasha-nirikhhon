import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';

/**
 * Components
 */
import {UsersComponent} from './components/users';
import {UserComponent} from './components/user';

import {AlbumsComponent} from './components/albums';
import {AlbumComponent} from './components/album';

import {PhotosComponent} from './components/photos';
import {PhotoComponent} from './components/photo';

import {SearchComponent} from './components/search';

/**
 * Views
 */
import {Page404} from './views/page-404';

const routes: Routes = [
  {path: '', redirectTo: '/users', pathMatch: 'full'},

  {path: 'users', component: UsersComponent},
  {path: 'user/:id', component: UserComponent},

  {path: 'albums', component: AlbumsComponent},
  {path: 'album/:id', component: AlbumComponent},

  {path: 'photos', component: PhotosComponent},
  {path: 'photo/:id', component: PhotoComponent},

  {path: 'search', component: SearchComponent},

  {path: '404', component: Page404},

  {path: '**', redirectTo: '/404'}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})

export class RoutingModule {
}