package use_case.addorcancelingredient;

import entity.Ingredient;

/**
 * DAO for the add ingredient Use Case.
 */
public interface AddorCancelIngredientIngredientDataAccessInterface {
    /**
     * Checks if the given ingredient name exists.
     * @param ingredientname the ingredient name to look for
     * @return true if an ingredient with the given ingredient name exists; false otherwise
     */
    boolean existsByIngredientName(String ingredientname);

    /**
     * Saves the ingredient.
     * @param ingredient the ingredient to save
     */
    void save(Ingredient ingredient);
}