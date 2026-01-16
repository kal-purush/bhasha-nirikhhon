/**
 * Created by matthewRanftle1 on 4/11/17.
 */
import { Routes, RouterModule } from '@angular/router';
import {LoginComponent} from "./components/login.component";
import {TestPhaseComponent} from "./components/testphase.component";
import {StudyPhaseComponent} from "./components/studyphase.component";
import {AuthGuard} from "./gaurds/auth.gaurd";
import {IntroComponent} from "./components/intro.component";


const appRoutes: Routes = [
  { path: 'intro', component: IntroComponent, canActivate:[AuthGuard]},
  { path: 'study-phase', component: StudyPhaseComponent, canActivate:[AuthGuard] },
  { path: 'test-phase', component: TestPhaseComponent, canActivate:[AuthGuard] },
  { path: 'login', component: LoginComponent},

  // otherwise redirect to home
  { path: '**', redirectTo: 'login' }
];

export const routing = RouterModule.forRoot(appRoutes);