import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { ImageComponent } from './image.component';

const imageRoutes: Routes = [
  { path: 'image', component: ImageComponent }
];

@NgModule({
  imports: [RouterModule.forChild(imageRoutes)],
  exports: [RouterModule]
})
export class ImageRoutingModule { }