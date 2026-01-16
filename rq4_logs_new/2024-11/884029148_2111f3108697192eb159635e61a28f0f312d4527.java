package use_case.expired_food;

import java.time.LocalDate;

public class ExpiredIngredientInputData {
    private final String ingredientName;
    private final LocalDate expiryDate;

    public ExpiredIngredientInputData(String ingredientName, LocalDate expiryDate) {
        this.ingredientName = ingredientName;
        this.expiryDate = expiryDate;
    }

    public String getIngredientName() {
        return ingredientName;
    }

    public LocalDate getExpiryDate() {
        return expiryDate;
    }
}