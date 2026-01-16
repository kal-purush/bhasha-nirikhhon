package use_case.delete_ingredient;

import data_access.InMemoryIngredientDataAccessObject;
import entity.*;
import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;

public class test_delete_ingredient {

    @Test
    public void successTest() {
        DeleteIngredientDataAccessInterface repository = new InMemoryIngredientDataAccessObject();
        InMemoryIngredientDataAccessObject ingredientRepository = new InMemoryIngredientDataAccessObject();

        Ingredient factory = new CommonIngredientFactory();
        Ingredient ingredient = factory.create("tomato", Local.parse("2024-12-11"));
        ingredientRepository.save(ingredient);

        DeleteIngredientInputData inputData = new DeleteIngredientInputData(
                ingredientRepository.getCurrentIngredients, ingredient);

        DeleteIngredientOutputBoundary successPresenter = new DeleteIngredientOutputBoundary() {
            @Override
            public void prepareSuccessView(DeleteIngredientOutputData outputData) {

                assertEquals(ingredient, outputData.getIngredient());
            }

            @Override
            public void prepareFailView() {
                //);
            }

            @Override
            public void switchToInitialView() {
                // This is expected
            }

            @Override
            public void switchToRecipeView() {
                // This is expected
            }
        };

        DeleteIngredientInputBoundary interactor = new DeleteIngredientInteractor(ingredientRepository, successPresenter);
        interactor.execute(inputData);
    }
}