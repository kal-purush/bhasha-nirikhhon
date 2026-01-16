package interface_adapter.recipe_search;

import use_case.recipe_search.RecipeSearchInputBoundary;
import use_case.recipe_search.RecipeSearchInputData;
import use_case.signup.SignupInputData;

public class RecipeSearchController {

    private final RecipeSearchInputBoundary searchUseCaseInteractor;

    public RecipeSearchController(RecipeSearchInputBoundary searchUseCaseInteractor) {
        this.searchUseCaseInteractor = searchUseCaseInteractor;
    }

    public void execute(String recipeName, String calMin, String calMax, String carbMin, String carbMax, String proteinMin, String proteinMax, String fatMin, String fatMax) {
        final RecipeSearchInputData recipeSearchInputData = new RecipeSearchInputData(
                recipeName, calMin, calMax, carbMin, carbMax, proteinMin, proteinMax, fatMin, fatMax);

        searchUseCaseInteractor.execute(recipeSearchInputData);
    }
    public void switchToLoginView() {
        searchUseCaseInteractor.switchToSearchResultsView();
    }
}