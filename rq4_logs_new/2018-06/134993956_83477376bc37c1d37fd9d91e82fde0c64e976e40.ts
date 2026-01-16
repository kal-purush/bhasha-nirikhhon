import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import { AuthGuard } from './auth/auth.guard';
import { WelcomeComponent } from './welcome/welcome.component';
// import {TrainingModule} from './training/training.module';


const routes: Routes = [
  {path: '', component: WelcomeComponent, canActivate: [AuthGuard]},
  {path: 'training', loadChildren: './training/training.module.ts'}
];

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [RouterModule],
  providers: [AuthGuard]
})
export class AppRoutingModule {
}