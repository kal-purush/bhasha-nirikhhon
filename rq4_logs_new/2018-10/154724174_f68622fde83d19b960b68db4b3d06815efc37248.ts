import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { Routes, RouterModule} from "@angular/router";
import { MaterialModule} from './material.module'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';

import { AppComponent } from './app.component';
import { NavbarComponent } from './components/navbar/navbar.component';
import { HomeComponent } from './pages/home/home.component';
import { LoginInputComponent } from './components/login-input/login-input.component';
import { ProfileComponent } from './pages/profile/profile.component';
import { AdminComponent } from './pages/admin-dashboard/admin.component';
import { SnacksComponent } from './pages/snacks/snacks.component';

import { AuthService } from './services/auth.service';
import { RouteGuardService } from './services/route-guard.service';

const routes: Routes = [
    { path: '', component: HomeComponent },
    { path: 'Hem', component:HomeComponent},
    { path: 'Profil', component: ProfileComponent, canActivate: [RouteGuardService]},
    { path: 'Admin', component: AdminComponent, canActivate: [RouteGuardService]},
    { path: 'Snacks', component: SnacksComponent, canActivate: [RouteGuardService]},
    {path: '**', redirectTo: ''}
   ];

@NgModule({
  declarations: [
    AppComponent,
    NavbarComponent,
    HomeComponent,
    LoginInputComponent,
    ProfileComponent,
    AdminComponent,
    SnacksComponent
  ],
  imports: [
    BrowserModule,
    RouterModule.forRoot(routes),
    MaterialModule,
    BrowserAnimationsModule,
    FormsModule,
    HttpClientModule
    
  ],
  entryComponents: [LoginInputComponent],
  providers: [RouteGuardService],
  bootstrap: [AppComponent]
})
export class AppModule { }