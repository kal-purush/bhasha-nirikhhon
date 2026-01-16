import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { FilterPostsPage } from './filter-posts';

@NgModule({
  declarations: [
    FilterPostsPage,
  ],
  imports: [
    IonicPageModule.forChild(FilterPostsPage),
  ],
})
export class FilterPostsPageModule {}