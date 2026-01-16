import { HomeComponent } from './home/home.component';
import { RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';
import { AboutComponent } from './about/about.component';
import { ResourcesComponent } from './resources/resources.component';
import { ArchiveComponent } from './archive/archive.component';
import { ContactComponent } from './contact/contact.component';

export const routes: Routes = [
  {
    path: 'blog',
    component: HomeComponent,
    pathMatch: 'full'
  }, {
    path: 'blog/about',
    component: AboutComponent,
    pathMatch: 'full'
  }, {
    path: 'blog/resources',
    component: ResourcesComponent,
    pathMatch: 'full'
  }, {
    path: 'blog/archive',
    component: ArchiveComponent,
    pathMatch: 'full'
  }, {
    path: 'blog/contact',
    component: ContactComponent,
    pathMatch: 'full'
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class BlogRoutingModule { }