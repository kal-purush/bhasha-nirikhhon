import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { extract } from '@app/i18n';
import { TopNewsComponent } from './top-news/top-news.component';
import { Shell } from '@app/shell/shell.service';

const routes: Routes = [
  Shell.childRoutes([
    { path: '', redirectTo: '/top-news', pathMatch: 'full' },
    { path: 'top-news', component: TopNewsComponent, data: { title: extract('Top News') } },
    { path: 'categories', component: TopNewsComponent, data: { title: extract('Categories') } },
    { path: 'search', component: TopNewsComponent, data: { title: extract('Search') } },
  ]),
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
  providers: [],
})
export class NewsRoutingModule {}