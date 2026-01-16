package data_access;

import entity.CommonRecipe;
import entity.Recipe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InMemoryRecipeManagementRepository {
    private static List<Recipe> initializeRecipes() {
        List<Recipe> recipeList = new ArrayList<>();
        recipeList.add(new CommonRecipe("Chocolate Cake", List.of("Delicious chocolate dessert", "Dessert"),
                Map.of("Flour", 200, "Sugar", 100, "Cocoa", 50)));
        recipeList.add(new CommonRecipe("Caesar Salad", List.of("Classic Caesar salad", "Appetizer"),
                Map.of("Lettuce", 100, "Croutons", 50, "Parmesan", 30)));
        recipeList.add(new CommonRecipe("Pancakes", List.of("Fluffy breakfast pancakes", "Breakfast"),
                Map.of("Flour", 150, "Milk", 200, "Eggs", 2)));
        return recipeList;
    }

    public List<Recipe> getCurrentRecipes() {
        return initializeRecipes();
    }
}