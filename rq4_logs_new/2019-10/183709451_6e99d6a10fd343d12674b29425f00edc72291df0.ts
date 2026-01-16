import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { ContentLayoutComponent } from '@layout/content-layout/content-layout.component';
import { AuthLayoutComponent } from '@layout/auth-layout/auth-layout.component';

import { appRoutesNames } from './app.routes.names';
import { NoAuthGuard } from '@app/guard/no-auth.guard';
import { AuthGuard } from '@app/guard/auth-guard';

const appRoutes: Routes = [
  {
    path: '',
    redirectTo: appRoutesNames.HOME,
    pathMatch: 'full'
  },
  {
    path: '',
    component: ContentLayoutComponent,
    canActivate: [NoAuthGuard],
    children: [
      {
        path: appRoutesNames.HOME,
        loadChildren: () => import('@modules/home/home.module').then(m => m.HomeModule)
      },
      {
        path: appRoutesNames.CLUBS,
        loadChildren: () => import('@modules/club/club.module').then(m => m.ClubModule)
      },
      {
        path: appRoutesNames.STATS,
        loadChildren: () => import('@modules/stats/stats.module').then(m => m.StatsModule)
      }
    ]
  },
  {
    path: appRoutesNames.ADMIN,
    component: AuthLayoutComponent,
    canActivate: [AuthGuard],
    loadChildren: () => import('@modules/admin/admin.module').then(m => m.AdminModule)
  },
  {
    path: '**',
    redirectTo: '/home',
    pathMatch: 'full'
  }
];

/**
 * Application Routes
 */
@NgModule( {
  imports: [
    RouterModule.forRoot(
      appRoutes,
      {
        // enableTracing: true, // <-- debugging purposes only
        // useHash: true
      }
    )
  ],
  exports: [
    RouterModule
  ],
  providers: []
} )
export class AppRoutingModule {}