package interface_adapter.addorcancelingredient;

import use_case.addorcancelingredient.AddorCancelIngredientInputBoundary;
import use_case.addorcancelingredient.AddorCancelIngredientInputData;

/**
 * Controller for the Add or cancel ingredient Use Case.
 */
public class AddorCancelIngredientController {

    private final AddorCancelIngredientInputBoundary addorcancelingredientUseCaseInteractor;

    public AddorCancelIngredientController(AddorCancelIngredientInputBoundary addorcancelingredientUseCaseInteractor) {
        this.addorcancelingredientUseCaseInteractor = addorcancelingredientUseCaseInteractor;
    }

    /**
     * Executes the add ingredient Use Case.
     * @param ingredientname the ingredient name
     * @param expirydate the expiry date
     */
    public void execute(String ingredientname, String expirydate) {
        final AddorCancelIngredientInputData addorCancelIngredientInputDataInputData = new AddorCancelIngredientInputData(
                ingredientname, expirydate);

        addorcancelingredientUseCaseInteractor.execute(addorCancelIngredientInputDataInputData);
    }

    /**
     * Executes the "switch to InitialView" Use Case.
     */
    public void switchToInitialView() {
        addorcancelingredientUseCaseInteractor.switchToInitialView();
    }
}