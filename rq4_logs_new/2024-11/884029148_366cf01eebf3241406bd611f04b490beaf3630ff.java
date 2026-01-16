package use_case.expired_food;

import entity.Ingredient;

import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;

public class CheckExpiredIngredientInteractor {
    private final CheckExpiredIngredientUserDataAccessInterface dataAccess;

    public CheckExpiredIngredientInteractor(CheckExpiredIngredientUserDataAccessInterface dataAccess) {
        this.dataAccess = dataAccess;
    }

    public List<Ingredient> execute() {
        List<Ingredient> ingredients = dataAccess.getAllIngredients();
        LocalDate today = LocalDate.now();

        List<Ingredient> expiredIngredients = new ArrayList<>();
        for (Ingredient ingredient : ingredients) {
            if (ingredient.getExpiryDate().isBefore(today) || ingredient.getExpiryDate().isEqual(today)) {
                expiredIngredients.add(ingredient);
            }
        }

        return expiredIngredients;
    }
}
