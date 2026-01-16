package data_access;

import entity.CommonRecipe;
import entity.Recipe;
import use_case.recipe_management.RecipeManagementUserDataAccessInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * In-memory implementation of the DAO for storing ingredient data. This implementation does
 * NOT persist data between runs of the program.
 */
public class InMemoryRecipeDataAccessObject implements RecipeManagementUserDataAccessInterface {
    private final List<Recipe> allRecipes = new ArrayList<>();

    @Override
    public List<Recipe> getCurrentRecipes(){
        return allRecipes;
    }

    @Override
    public void saveRecipe(Recipe recipe) {
        allRecipes.add(recipe);
    }

    public void populateAllRecipes() {
        allRecipes.add(new CommonRecipe("KungPowChicken", List.of("this is a traditional Chinese dish", "Chinese"), Map.of("chicken", 2, "tomato", 3)));
    }
}