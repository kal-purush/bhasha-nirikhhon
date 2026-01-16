package use_case.recipe_management;

import entity.Recipe;
import java.util.List;

/**
 * The input data for the Recipe Management Use Case.
 */
public class RecipeManagementInputData {

    private List<Recipe> recipes;
    private String filterCategory;

    public RecipeManagementInputData(List<Recipe> recipes, String filterCategory) {
        this.recipes = recipes;
        this.filterCategory = filterCategory;
    }

    public List<Recipe> getRecipes() {
        return recipes;
    }

    public void setRecipes(List<Recipe> recipes) {
        this.recipes = recipes;
    }

    public String getFilterCategory() {
        return filterCategory;
    }

    public void setFilterCategory(String filterCategory) {
        this.filterCategory = filterCategory;
    }

}