import { NgModule } from '@angular/core';
import { AuthGuard } from './auth.guard';
import { PathLocationStrategy, LocationStrategy } from '@angular/common';
import { HTTP_INTERCEPTORS } from '@angular/common/http';
import { RouterModule, Routes } from '@angular/router';

import { PathResolveService } from './service/path-resolve.service';
import { AuthenticationService } from './service/authentication.service';

import { HomepageComponent } from './page/homepage/homepage.component';
import { AdminpageComponent } from './page/adminpage/adminpage.component';
import { SignuppageComponent } from './page/signuppage/signuppage.component';
import { UpdatepageComponent } from './page/updatepage/updatepage.component';
import { ProfilepageComponent } from './page/profilepage/profilepage.component';
import { NotFoundComponent } from './page/not-found/not-found.component';
import { paths } from './object/paths';

const appRoutes: Routes = [
  {
      path: '',
      redirectTo: paths.shop+'/'+paths.content,
      pathMatch: 'full'
  },
  {
      path: paths.shop,
      canActivate: [AuthGuard],
      children: [
          {
              path: '',
              redirectTo: paths.content,
              pathMatch: 'full'
          },
          {
              path: paths.content,
              component: HomepageComponent
          },
          {
              path: paths.profile,
              component: ProfilepageComponent
          },
          {
              path: paths.admin,
              component: AdminpageComponent
          },
          {
              path: paths.signup,
              component: SignuppageComponent
          },
          {
              path: paths.update,
              component: UpdatepageComponent
          }            
      ]
  },
  {
    path: '**',
    canActivate: [AuthGuard],
    resolve: {
      path: PathResolveService
    },
    component: NotFoundComponent
  }
];

@NgModule({
  imports: [RouterModule.forRoot(appRoutes, {useHash: true})],
  providers: [
    {
        provide: HTTP_INTERCEPTORS,
        useClass: AuthenticationService,
        multi: true
    },
    {provide: LocationStrategy, useClass: PathLocationStrategy},
  ],
  exports: [RouterModule]
})
export class AppRoutingModule { }