import { HomeComponent } from './home/home.component';
import { RecipeListComponent } from './recipes/recipe-list/recipe-list.component';
import { ShoppingEditComponent } from './shopping-list/shopping-edit/shopping-edit.component';
import { RecipeDetailComponent } from './recipes/recipe-detail/recipe-detail.component';
import { ShoppingListComponent } from './shopping-list/shopping-list.component';
import { RecipesComponent } from './recipes/recipes.component';
import { HeaderComponent } from './header/header.component';
import { RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';

const appRoutes: Routes = [
  {
    path: '',
    component: HomeComponent,
    children: [
    ]
  },
  {
    path: 'recipes',
    component: RecipesComponent,
    children: [
      { path: ':id', component: RecipeDetailComponent }
      // { path: ':id/edit', component: RecipeEditComponent } --after RecipeEDitComponen set comment out here
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