import { Routes } from '@angular/router';

export const RoutesConfig: Routes = [
    {
      path: '',
      loadChildren: () => import('./features/home/home.module').then(m => m.HomeModule)
    },
    {
      path: 'teams',
      loadChildren: () => import('./features/teams/teams.module').then(m => m.TeamsModule)
    },
    {
      path: 'teams/:id',
      loadChildren: () => import('./features/team/team.module').then(m => m.TeamModule)
    },
    {
      path: 'standings',
      loadChildren: () => import('./features/standings/standings.module').then(m => m.StandingsModule)
    },
    {
      path: 'matches',
      loadChildren: () => import('./features/matches/matches.module').then(m => m.MatchesModule)
    },
    {
      path: 'favorites',
      loadChildren: () => import('./features/favorites/favorites.module').then(m => m.FavoritesModule)
    }
  ];