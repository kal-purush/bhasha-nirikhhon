import {Injectable} from "@angular/core";
import {Ingredient} from "../../core/models/ingredient.model";
import {Subject} from "rxjs";

@Injectable({
  providedIn: 'root'
})
export class ShoppingListService {

  ingredientsChanged = new Subject<Array<Ingredient>>();
  ingredientEdited =new Subject<number>();

  private ingredients: Array<Ingredient> = [
    new Ingredient('Apples', 5),
    new Ingredient('Tomatoes', 10)
  ];

  getIngredient(index: number): Ingredient {
    return this.ingredients[index];
  }

  getIngredients(): Array<Ingredient> {
    return this.ingredients.slice();
  }

  addIngredient(ingredient: Ingredient): void {
    this.ingredients.push(ingredient);
    this.ingredientsChanged.next(this.getIngredients());
  }

  addIngredients(ingredients: Array<Ingredient>): void {
    this.ingredients.push(...ingredients);
    this.ingredientsChanged.next((this.getIngredients()));
  }

  updateIngredient(index: number, ingredient: Ingredient): void {
    this.ingredients[index] = ingredient;
    this.ingredientsChanged.next((this.getIngredients()));
  }

  deleteIngredient(index: number) {
    this.ingredients.splice(index, 1);
    this.ingredientsChanged.next((this.getIngredients()));
  }
}