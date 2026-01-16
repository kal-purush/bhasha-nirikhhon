import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { DisplayPostDislikesPage } from './display-post-dislikes';

@NgModule({
  declarations: [
    DisplayPostDislikesPage,
  ],
  imports: [
    IonicPageModule.forChild(DisplayPostDislikesPage),
  ],
})
export class DisplayPostDislikesPageModule {}