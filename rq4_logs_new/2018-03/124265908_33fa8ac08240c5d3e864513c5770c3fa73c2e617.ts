import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { ShinglesComponent } from './components/shingles/shingles.component';
import { ReviewsComponent } from './components/reviews/reviews.component';
import { ListComponent } from './components/list/list.component';

const routes: Routes = [
  {
    path: 'shingles',
    component: ShinglesComponent,
    pathMatch: 'full'
  },
  {
    path: 'reviews',
    component: ReviewsComponent
  },
  {
    path: 'list',
    component: ListComponent
  },
  {
    path: '**', redirectTo: 'shingles'
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }