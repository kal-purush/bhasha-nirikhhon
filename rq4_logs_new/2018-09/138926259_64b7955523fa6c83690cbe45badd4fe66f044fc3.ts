import { RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';

import { EmptyRecipeComponent } from './recipes/empty-recipe/empty-recipe.component';
import { ShoppingEditComponent } from './shopping-list/shopping-edit/shopping-edit.component';
import { RecipeDetailComponent } from './recipes/recipe-detail/recipe-detail.component';
import { ShoppingListComponent } from './shopping-list/shopping-list.component';
import { RecipesComponent } from './recipes/recipes.component';
import { RecipeEditComponent } from './recipes/recipe-edit/recipe-edit.component';

const appRoutes: Routes = [
  {
    path: '',
    redirectTo: '/recipes',
    pathMatch: 'full'
  },
  {
    path: 'recipes',
    component: RecipesComponent,
    children: [
      { path: '', component: EmptyRecipeComponent, pathMatch: 'full' },
      { path: 'new', component: RecipeEditComponent }, // static parameter should come before dynamic parameter
      { path: ':id', component: RecipeDetailComponent },
      { path: ':id/edit', component: RecipeEditComponent }
    ]
  },
  {
    path: 'shopping-list',
    component: ShoppingListComponent,
    children: [{ path: ':id/edit', component: ShoppingEditComponent }]
  },
  { path: '**', redirectTo: '' }
];

@NgModule({
  declarations: [],
  imports: [RouterModule.forRoot(appRoutes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}