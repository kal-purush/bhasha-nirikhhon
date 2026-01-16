import { NgModule }             from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { RetroInputComponent } from './retro-input/retro-input.component';
import { AllStickiesComponent } from './all-stickies/all-stickies.component';
import {  AlwaysAuthGuard } from './authentication/alwaysauthguard';
import { OnlyLoggedInUsersGuard } from './authentication/onlyloggedInusersguard';

const routes: Routes = [
    {path: '', redirectTo: 'home', pathMatch: 'full'},
    {path: 'home', component: RetroInputComponent, pathMatch: 'full'},
    {path: 'all', component: AllStickiesComponent, pathMatch: 'full' , canActivate: [OnlyLoggedInUsersGuard , AlwaysAuthGuard]},
    {path: '**', redirectTo: 'home'}

];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}