package homeworks.homework20;

import homeworks.homework20.burgerEnum.BurgerSize;
import homeworks.homework20.burgerEnum.Ingredient;
import homeworks.homework20.burgerEnum.MeatType;

import java.util.Collections;
import java.util.List;

public class Burger {
    private final BurgerSize burgerSize;
    private final MeatType meatType;
    private final List<Ingredient> ingredients;

    public Burger(BurgerSize burgerSize, MeatType meatType, List<Ingredient> ingredients) {
        this.burgerSize = burgerSize;
        this.meatType = meatType;
        this.ingredients = ingredients;
    }

    public void displayBurgerInfo() {
        StringBuilder burgerIngredients = new StringBuilder();
        int cheeseFrequency = Collections.frequency(ingredients, Ingredient.CHEESE);
        int tomatoFrequency = Collections.frequency(ingredients, Ingredient.TOMATO);
        int lettuceFrequency = Collections.frequency(ingredients, Ingredient.LETTUCE);
        if (cheeseFrequency > 0) {
            burgerIngredients.append(cheeseFrequency + "x-" + Ingredient.CHEESE + " ");
        }
        if (tomatoFrequency > 0) {
            burgerIngredients.append(tomatoFrequency + "x-" + Ingredient.TOMATO + " ");
        }
        if (lettuceFrequency > 0) {
            burgerIngredients.append(lettuceFrequency + "x-" + Ingredient.LETTUCE + " ");
        }
        System.out.println("Your order is: " + burgerSize + " size burger with " + meatType +
                " and ingredients: " + burgerIngredients);
        double ingredientsCost = 0.00;
        for (Ingredient ingredient : ingredients) {
            ingredientsCost += ingredient.getCost();
        }
        double totalBurgerCost = burgerSize.getCost() + meatType.getCost() + ingredientsCost;
        System.out.println("Total burger cost: " + totalBurgerCost);
    }
}