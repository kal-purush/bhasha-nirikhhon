
import { Routes, RouterModule } from '@angular/router';
import { HomeComponent } from 'src/app/components/home/home.component';
import { LoginComponent } from 'src/app/components/login/login.component';
import { GithubComponent } from 'src/app/components/github/github.component';
import { NotFoundComponent } from 'src/app/components/not-found/not-found.component';

export const routing = RouterModule.forRoot([
  {path: '', component: HomeComponent},
  {path: 'github', component: GithubComponent},
  {path: 'login', component: LoginComponent},
  {path: '**', component:NotFoundComponent}
]);