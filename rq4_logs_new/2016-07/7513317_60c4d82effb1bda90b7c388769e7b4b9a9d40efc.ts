import { provideRouter, RouterConfig } from '@angular/router';
import { HomeComponent } from './+home';
import { ExperienceComponent } from './+experience';
import { SkillsComponent } from './+skills';
import { DemosComponent } from './+demos';
import { BlogComponent } from './+blog';
import { ResumeComponent } from './+resume';

export const routes: RouterConfig = [
  {path: 'home', component: HomeComponent},
  {path: 'experience', component: ExperienceComponent},
  {path: 'skills', component: SkillsComponent},
  {path: 'demos', component: DemosComponent},
  {path: 'blog', component: BlogComponent},
  {path: 'resume', component: ResumeComponent},
  {path: '**', redirectTo: '/home', pathMatch: 'full'}
];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];