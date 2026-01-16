package interface_adapter.search_results;

import use_case.search_results.SearchResultsInputBoundary;

public class SearchResultsController {

    private final SearchResultsInputBoundary searchResultsInputBoundary;

    public SearchResultsController(SearchResultsInputBoundary searchResultsInputBoundary) {
        this.searchResultsInputBoundary = searchResultsInputBoundary;
    }

    public void switchtoRecipeSearchView(){
        this.searchResultsInputBoundary.switchToRecipeSearchView();
    }
}