import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { RouterModule } from '@angular/router';
import { ReactiveFormsModule } from  '@angular/forms'
import { HTTP_INTERCEPTORS } from '@angular/common/http'


import { UserService } from './services/user.service';
import { AuthService } from './services/auth.service';
//import { TokenInterceptor } from './token.interceptor';

import { AppComponent } from './app.component';
import { LoginComponent } from './login/login.component';
import { WelcomeComponent } from './welcome/welcome.component';
import { RegistrationComponent } from './registration/registration.component';
import { NavbarComponent } from './navbar/navbar.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { GameBoardComponent } from './game-board/game-board.component';
import { CellComponent } from './cell/cell.component';
//import { RecoverAccountComponent } from './recover-account/recover-account.component';


@NgModule({
  declarations: [
    AppComponent,
    LoginComponent,
    WelcomeComponent,
    RegistrationComponent,
    NavbarComponent,
    DashboardComponent,
    GameBoardComponent,
    CellComponent,
  
    
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule,
    ReactiveFormsModule,
    RouterModule.forRoot([
      {
        path: '',
        component: WelcomeComponent
      },
      {
        path: 'login',
        component: LoginComponent
      },
      {
        path: 'register',
        component: RegistrationComponent
      },
      {
        path: 'dashboard',
        component: DashboardComponent
      },
      {
        path: 'game',
        component: GameBoardComponent
      }
      // {
      //   path: 'accountrecovery',
      //   component: RecoverAccountComponent
      // }
    ])
  ],
  providers: [
    UserService,
    AuthService,
    // {
    //   provide: HTTP_INTERCEPTORS,
    //   //useClass: TokenInterceptor,
    //   // multi: true
    // }
],
  
  bootstrap: [AppComponent]
})
export class AppModule { }