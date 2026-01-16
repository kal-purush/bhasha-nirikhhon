package interface_adapter.deleteingredient;

import entity.Ingredient;
import use_case.delete_ingredient.DeleteIngredientInputBoundary;
import use_case.delete_ingredient.DeleteIngredientInputData;
import java.util.List;

/**
 * The controller for the Delete Ingredient Use Case.
 */
public class DeleteIngredientController {

    private final DeleteIngredientInputBoundary deleteIngredientUseCaseInteractor;

    public DeleteIngredientController(DeleteIngredientInputBoundary deleteIngredientUseCaseInteractor) {
        this.deleteIngredientUseCaseInteractor = deleteIngredientUseCaseInteractor;
    }

    /**
     * Executes the Delete Ingredient Use Case.
     * @param ingredients the ingredients in the user's fridge
     * @param ingredient the ingredient needs to be deleted
     */
    public void execute(List<Ingredient> ingredients, Ingredient ingredient) {
        final DeleteIngredientInputData deleteIngredientInputData = new DeleteIngredientInputData(ingredients,
                ingredient);
       deleteIngredientUseCaseInteractor.execute(deleteIngredientInputData);
    }
}