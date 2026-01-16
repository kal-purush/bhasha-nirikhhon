package interface_adapter.deleteingredient;

import interface_adapter.ViewManagerModel;
import interface_adapter.inital.InitialState;
import interface_adapter.inital.InitialViewModel;
import use_case.delete_ingredient.DeleteIngredientOutputBoundary;
import use_case.delete_ingredient.DeleteIngredientOutputData;

/**
 * The Presenter for the Delete Ingredient Use Case.
 */
public class DeleteIngredientPresenter implements DeleteIngredientOutputBoundary {
    private final InitialViewModel viewModel;
    private final ViewManagerModel viewManagerModel;

    public DeleteIngredientPresenter(InitialViewModel viewModel, ViewManagerModel viewManagerModel) {
        this.viewModel = viewModel;
        this.viewManagerModel = viewManagerModel;
    }

    @Override
    public void prepareSuccessView(DeleteIngredientOutputData response){
        final InitialState initialState = viewModel.getState();
        initialState.setIngredients(response.getIngredients());
        this.viewModel.setState(initialState);
        this.viewModel.firePropertyChanged();

        this.viewManagerModel.setState(viewModel.getViewName());
        this.viewManagerModel.firePropertyChanged();
    }

    @Override
    public void prepareFailView(String errorMessage){
        // No such a case
    }
}