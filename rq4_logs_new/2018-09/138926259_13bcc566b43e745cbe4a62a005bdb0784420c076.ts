import { EventEmitter } from '@angular/core';
import { Recipe } from './recipe.model';
import { Ingredient } from '../shared/ingredient.model';


export class RecipeService {
    selectedRecipe = new EventEmitter<Recipe>();

    private recipes = [
        new Recipe('A Test Recipe',
            'This si simply a test',
            'http://cdn-img.health.com/sites/default/files/styles/large_16_9/public/1470949021/crispy-farro-shrimp-stir-fry-bowel-recipe.jpg',
            [
                new Ingredient('test1', 1),
                new Ingredient('test2', 2)]),
        new Recipe('A Test Recipe',
            'This si simply a test',
            'http://cdn-img.health.com/sites/default/files/styles/large_16_9/public/1470949021/crispy-farro-shrimp-stir-fry-bowel-recipe.jpg',
            [
                new Ingredient('test3', 3),
                new Ingredient('test4', 4)])
    ];
    getRecipes() {
        return this.recipes.slice();
    }
}