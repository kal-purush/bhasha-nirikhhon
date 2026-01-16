package interface_adapter.recipe_search;

import interface_adapter.ViewManagerModel;
import interface_adapter.search_results.SearchResultsState;
import interface_adapter.search_results.SearchResultsViewModel;
import use_case.recipe_search.RecipeSearchOutputBoundary;
import use_case.recipe_search.RecipeSearchOutputData;


public class RecipeSearchPresenter implements RecipeSearchOutputBoundary {

    private final RecipeSearchViewModel recipeSearchviewModel;
    private final SearchResultsViewModel searchResultsViewModel;
    private final ViewManagerModel viewManagerModel;

    public RecipeSearchPresenter(RecipeSearchViewModel recipeSearchviewModel, SearchResultsViewModel searchResultsViewModel, ViewManagerModel viewManagerModel) {
        this.recipeSearchviewModel = recipeSearchviewModel;
        this.searchResultsViewModel = searchResultsViewModel;
        this.viewManagerModel = viewManagerModel;
    }

    @Override
    public void prepareSuccessView(RecipeSearchOutputData outputData) {
        // On success, switch to the login view.
        final SearchResultsState searchResultsState = searchResultsViewModel.getState();
        searchResultsState.setSearchResults(outputData.getSearchResults());
        this.searchResultsViewModel.setState(searchResultsState);
        searchResultsViewModel.firePropertyChanged();

        viewManagerModel.setState(searchResultsViewModel.getViewName());
        viewManagerModel.firePropertyChanged();
    }

    @Override
    public void prepareFailView(String errorMessage) {

    }

    @Override
    public void switchToLoginView() {
        viewManagerModel.setState(searchResultsViewModel.getViewName());
        viewManagerModel.firePropertyChanged();
    }
}