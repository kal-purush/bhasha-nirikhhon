import { APP_BASE_HREF, CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { BookContainerComponent } from './books/components/book-container/book-container.component';
import { LoginComponent } from './login/components/login/login.component';
import { AuthGuardService } from './shared/guards/auth-guard.service';
import { LoginGuardService } from './shared/guards/login-guard.service';

const appRoutes: Routes = [
  { path: 'login', component: LoginComponent, canActivate: [LoginGuardService] },
  { path: 'books', component: BookContainerComponent, canActivate: [AuthGuardService] },
  { path: '', redirectTo: '/login', pathMatch: 'full' }
];

@NgModule({
  imports: [
    CommonModule,
    RouterModule.forRoot(appRoutes, {useHash: true}),
  ],
  declarations: [],
  exports: [
    RouterModule
  ],
  providers: [
    {
      provide: APP_BASE_HREF,
      useValue: '/'
    },
  ]
})
export class AppRoutingModule { }