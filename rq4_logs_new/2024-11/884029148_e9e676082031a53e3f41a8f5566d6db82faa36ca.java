package use_case.delete_ingredient;

import entity.Ingredient;

import java.util.List;

/**
 * DAO for the Delete Ingredient Use Case.
 */
public interface DeleteIngredientUserDataAccessInterface {

    /**
     * Returns the ingredients of the current user of the application.
     * @return the ingredients of the current user; empty list indicates that current user has not added any ingredient.
     */
    List<Ingredient> getCurrentIngredients();

    /**
     * Sets the ingredients of the current user after removing ingredient.
     * @param ingredients the new current ingredients; empty list indicates that current user has not added any
     *                    ingredient.
     */
    void setCurrentIngredients(List<Ingredient> ingredients);
}