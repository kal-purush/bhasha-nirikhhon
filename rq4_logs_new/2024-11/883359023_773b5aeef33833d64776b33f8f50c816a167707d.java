package interface_adapter.recipe_search;

import use_case.signup.SignupInputBoundary;
import use_case.signup.SignupInputData;

/**
 * Controller for the Recipe Search Use Case.
 */
public class RecipeSearchController {

    public RecipeSearchController() {

    }

    /**
     * Executes the Recipe Search Use Case.
     * @param title the recipe the user wants
     * @param calorieInfo the password
     * @param carbInfo the password
     * @param password2 the password repeated
     *
     *                          this.add(title);
     *         this.add(recipeInfo);
     *         this.add(calorieInfo);
     *         this.add(carbInfo);
     *         this.add(proteinInfo);
     *         this.add(fatInfo);
     */
    public void execute(String title, String password1, String password2) {
        final SignupInputData signupInputData = new SignupInputData(
                username, password1, password2);

        userSignupUseCaseInteractor.execute(signupInputData);
    }
}