import { Ingredient } from '../shared/ingredient.model';

export class ShoppingService {
    ingredients: Ingredient[] = [
        new Ingredient('Apples', 5),
        new Ingredient('Tomatoes', 10),
      ];
    addIngredient(ingredientName: Ingredient) {
        this.ingredients.push(ingredientName);
    }
}