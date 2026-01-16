import { NgModule } from "@angular/core";
import { Routes, RouterModule, Router } from "@angular/router";
import { ShoppingListComponent } from "./shopping/shopping-list/shopping-list.component";
import {RecipesComponent} from "./recipes/recipes.component";
import { RecipeItemComponent } from './recipes/recipe-list/recipe-item/recipe-item.component';
import { RecipeCreateComponent } from './recipes/recipe-create/recipe-create/recipe-create.component';
import { RecipeDetailComponent } from './recipes/recipe-detail/recipe-detail.component';
const appRoutes:Routes = [
    {path: '',pathMatch:'full', redirectTo:'recipes'},
    {path:'recipes', component:RecipesComponent, children:[   
        {path:'create-recipe',component:RecipeCreateComponent},
        {path:':id', component:RecipeDetailComponent},
        {path:':id/edit', component:RecipeCreateComponent},
    ]},
    {path:'shopping-list', component:ShoppingListComponent,},
];
@NgModule({
    imports:[RouterModule.forRoot(appRoutes)],
    exports:[RouterModule]
})
export class AppRoutingModule{

}