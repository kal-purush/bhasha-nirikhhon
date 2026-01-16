package use_case.review_recipe;

import use_case.signup.SignupOutputData;

/**
 * The output boundary for the Recipe Review Use Case.
 */
public interface RecipeReviewOutputBoundary {
    /**
     * Prepares the success view for the Recipe Review Use Case.
     * @param outputData the output data
     */
    void prepareSuccessView(SignupOutputData outputData);

    /**
     * Prepares the failure view for the Recipe Review Use Case.
     * @param errorMessage the explanation of the failure
     */
    void prepareFailView(String errorMessage);

    /**
     * Switches to the Recipe History View.
     */
    void switchToRecipeHistoryView();
}