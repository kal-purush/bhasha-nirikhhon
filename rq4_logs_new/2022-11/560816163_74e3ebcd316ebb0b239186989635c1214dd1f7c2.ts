import { EventEmitter } from "@angular/core";
import { Recipe } from "./recipe.model";

export class RecipeService{
    recipeSelected = new EventEmitter<Recipe>;
    private recipes:Recipe[]=[
        new Recipe('A Test Recipe','This is simply a test','https://lh3.googleusercontent.com/BOuh1_D9cyTHgNv5g8j3HRwW8BzPvAq3i9WV9xgFvm1hxQ_IlhnvZVGS4aAUPRGaElFzHpV6lQ8XWRPGcluZhM0InX0EUB0oh3vU=w220-h220-c-rw-v1-e365'),
        new Recipe('Another Test Recipe','This is simply a test','https://lh3.googleusercontent.com/BOuh1_D9cyTHgNv5g8j3HRwW8BzPvAq3i9WV9xgFvm1hxQ_IlhnvZVGS4aAUPRGaElFzHpV6lQ8XWRPGcluZhM0InX0EUB0oh3vU=w220-h220-c-rw-v1-e365') 
      ];

    getRecipes(){
        // We use slice() to get a new array that is a copy of the original
        // changing this array won't change the original one.
        return this.recipes.slice();
    }

}