import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { ProjectsComponent } from '../projects/projects.component';
import { AboutMeComponent } from '../about-me/about-me.component';
import { EntrepreneurComponent } from '../entrepreneur/entrepreneur.component';
import { NewProjectComponent } from '../projects/new-project/new-project.component';

import { ProjectDetailComponent } from '../projects/project-detail/project-detail.component';

const appRoutes: Routes = [
  { path: '', redirectTo: '/projects', pathMatch: 'full' },
  { path: 'projects', component: ProjectsComponent, children: [
    { path: 'new', component: NewProjectComponent}
    // { path: '/:id', component: ProjectDetailComponent}
  ] },
  { path: 'about-me', component: AboutMeComponent},
  { path: 'entrepreneur', component: EntrepreneurComponent}
]

@NgModule({
  imports: [RouterModule.forRoot(appRoutes)],
  exports: [RouterModule]
})

export class AppRoutingModule {}