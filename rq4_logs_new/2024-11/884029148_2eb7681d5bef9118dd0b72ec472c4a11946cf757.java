package interface_adapter.ExpirationWarning;
import entity.Ingredient;
import use_case.expired_food.CheckExpiredIngredientInteractor;
import java.util.List;

class ExpirationWarningController {
    private final CheckExpiredIngredientInteractor interactor;

    public ExpirationWarningController(CheckExpiredIngredientInteractor interactor) {
        if (interactor == null) {
            throw new IllegalArgumentException("Interactor cannot be null");
        }
        this.interactor = interactor;
    }

    public List<Ingredient> getExpiredIngredients() {
        return interactor.execute();
    }

    public void deleteSelectedIngredients(List<Ingredient> ingredientsToDelete) {
        interactor.deleteIngredients(ingredientsToDelete);
    }
}
