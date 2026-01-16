import { EventEmitter } from "@angular/core";
import { Recipe } from "./recipe-list/recipe.model";

export class RecipesService{
    recipeSelected=new EventEmitter<Recipe>();
    
    private recipes : Recipe[]=[
        new Recipe("Recipe1", "Tzuchini recipe", "https://theabsolutefoodie.com/wp-content/uploads/2022/07/indian-zucchini-recipes.jpg"),
        new Recipe("Recipe2", "Tartar recipe", "https://www.inspiredtaste.net/wp-content/uploads/2019/02/Homemade-Tartar-Sauce-Recipe-1200.jpg"),
      ];

      getRecipes(): Recipe[]{
        return this.recipes.slice();
      }

}