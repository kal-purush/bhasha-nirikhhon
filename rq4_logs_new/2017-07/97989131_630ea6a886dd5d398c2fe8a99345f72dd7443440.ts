import { ModuleWithProviders } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { CharactersListComponent } from './characters-list/characters-list.component';
import { CharacterDetailsComponent } from './character-details/character-details.component';
import { ComicsListComponent } from './comics-list/comics-list.component';
import { ComicDetailsComponent } from './comic-details/comic-details.component';
import { CreatorsListComponent } from './creators-list/creators-list.component';
import { CreatorDetailsComponent } from './creator-details/creator-details.component';
import { EventsListComponent } from './events-list/events-list.component';
import { EventDetailsComponent } from './event-details/event-details.component';
import { SeriesListComponent } from './series-list/series-list.component';
import { SeriesDetailsComponent } from './series-details/series-details.component';

const appRoutes: Routes = [
  { path: '', redirectTo: '/characters', pathMatch: 'full' },
  { path: 'characters', component: CharactersListComponent },
  { path: 'characters/:id', component: CharacterDetailsComponent },
  { path: 'comics', component: ComicsListComponent },
  { path: 'comics/:id', component: ComicDetailsComponent },
  { path: 'creators', component: CreatorsListComponent },
  { path: 'creators/:id', component: CreatorDetailsComponent },
  { path: 'events', component: EventsListComponent },
  { path: 'events/:id', component: EventDetailsComponent },
  { path: 'series', component: SeriesListComponent },
  { path: 'series/:id', component: SeriesDetailsComponent }
];

export const appRoutingProviders: any[] = [

];

export const routing: ModuleWithProviders = RouterModule.forRoot(appRoutes);