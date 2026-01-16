import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HeroComponent } from './forefront/hero/hero.component';
import { HeroesComponent } from './forefront/heroes/heroes.component';
import { PostComponent } from './forefront/post/post.component';
import { PostsComponent } from './forefront/posts/posts.component';
import { SearchResultsComponent } from './forefront/search-results/search-results.component';
import { AboutComponent } from './forefront/about/about.component';


const routes: Routes = [
    { path: '', redirectTo: '/heroes', pathMatch: 'full' },
    { path: 'hero/:id', component: HeroComponent },
    { path: 'heroes', component: HeroesComponent },
    { path: 'blog/:id', component: PostComponent },
    { path: 'blog', component: PostsComponent },
    { path: 'about', component: AboutComponent },
    { path: 'search', component: SearchResultsComponent },
    { path: '**', component: HeroesComponent }

];

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [
    RouterModule
  ]
})
export class AppRoutingModule { }