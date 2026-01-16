package com.novi.eindopdracht.recipeDiary.recipeDiary.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RecipeTest {

    @Test
    void shouldReturnRecipeId() {
        // Arrange
        Recipe recipe = new Recipe();
        Long id = 1L;

        // Act
        recipe.setRecipeId(id);

        // Assert
        assertEquals(id, recipe.getRecipeId());
    }

    @Test
    void shouldReturnRecipeName() {
        // Arrange
        Recipe recipe = new Recipe();
        String name = "Delicious Recipe";

        // Act
        recipe.setName(name);

        // Assert
        assertEquals(name, recipe.getName());
    }

    @Test
    void shouldReturnIngredientsOfRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        List<Ingredient> ingredients = new ArrayList<>();
        ingredients.add(new Ingredient());
        ingredients.add(new Ingredient());

        // Act
        recipe.setIngredients(ingredients);

        // Assert
        assertEquals(ingredients, recipe.getIngredients());
    }

    @Test
    void shouldReturnPrepTimeForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        String prepTime = "30 minutes";

        // Act
        recipe.setPrepTime(prepTime);

        // Assert
        assertEquals(prepTime, recipe.getPrepTime());
    }

    @Test
    void shouldReturnServingsForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        int servings = 4;

        // Act
        recipe.setServings(servings);

        // Assert
        assertEquals(servings, recipe.getServings());
    }

    @Test
    void shouldReturnNotesForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        String notes = "Some important notes.";

        // Act
        recipe.setNotes(notes);

        // Assert
        assertEquals(notes, recipe.getNotes());
    }

    @Test
    void shouldReturnTagForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        String tag = "Dessert";

        // Act
        recipe.setTag(tag);

        // Assert
        assertEquals(tag, recipe.getTag());
    }

    @Test
    void shouldReturnRatingForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        int rating = 5;

        // Act
        recipe.setRating(rating);

        // Assert
        assertEquals(rating, recipe.getRating());
    }

    @Test
    void shouldReturnRecipeSourceForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        String source = "https://example.com";

        // Act
        recipe.setRecipeSource(source);

        // Assert
        assertEquals(source, recipe.getRecipeSource());
    }

    @Test
    void shouldReturnCategoryNameForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        String categoryName = "Asian";

        // Act
        recipe.setCategoryName(categoryName);

        // Assert
        assertEquals(categoryName, recipe.getCategoryName());
    }

    @Test
    void shouldReturnShoppingListForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        ShoppingList shoppingList = new ShoppingList();

        // Act
        recipe.setShoppingList(shoppingList);

        // Assert
        assertEquals(shoppingList, recipe.getShoppingList());
    }

    @Test
    void shouldReturnPhotoForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        Photo photo = new Photo();

        // Act
        recipe.setPhoto(photo);

        // Assert
        assertEquals(photo, recipe.getPhoto());
    }

    @Test
    void shouldReturnRecipeDiaryForRecipe() {
        // Arrange
        Recipe recipe = new Recipe();
        RecipeDiary recipeDiary = new RecipeDiary();

        // Act
        recipe.setRecipeDiary(recipeDiary);

        // Assert
        assertEquals(recipeDiary, recipe.getRecipeDiary());
    }
}