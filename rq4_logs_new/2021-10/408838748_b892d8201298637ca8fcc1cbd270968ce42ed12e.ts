import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AddCategoryComponent } from './crud/add-category/add-category.component';
import { CRUDComponent } from './crud/crud.component';
import { EditCategoryComponent } from './crud/edit-category/edit-category.component';

const routes: Routes = [
  {path: 'skillmatrix/categories', component: CRUDComponent},
  {path: 'skillmatrix/categories/add', component: AddCategoryComponent},
  {path: 'skillmatrix/categories/:id', component: EditCategoryComponent}
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class CategoriesRoutingModule { }