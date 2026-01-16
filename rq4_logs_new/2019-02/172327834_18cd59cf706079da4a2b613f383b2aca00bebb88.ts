import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { NewsComponent } from './news/news.component';
import { ViewNewsArticleComponent } from './view-news-article/view-news-article.component';

const routes: Routes = [
  { path: 'news', component: NewsComponent },
  { path: 'news/:id', component: ViewNewsArticleComponent }
];

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [ RouterModule ]
})

export class AppRoutingModule {

}