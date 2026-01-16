package com.novi.eindopdracht.recipeDiary.recipeDiary.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IngredientTest {

    @Test
    void shouldReturnIngredientId() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        Long id = 1L;

        // Act
        ingredient.setIngredientId(id);

        // Assert
        assertEquals(id, ingredient.getIngredientId());
    }

    @Test
    void shouldReturnIngredientName() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        String name = "Sugar";

        // Act
        ingredient.setIngredientName(name);

        // Assert
        assertEquals(name, ingredient.getIngredientName());
    }

    @Test
    void shouldReturnQuantity() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        float quantity = 100.0f;

        // Act
        ingredient.setQuantity(quantity);

        // Assert
        assertEquals(quantity, ingredient.getQuantity());
    }

    @Test
    void shouldReturnUnit() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        String unit = "grams";

        // Act
        ingredient.setUnit(unit);

        // Assert
        assertEquals(unit, ingredient.getUnit());
    }

    @Test
    void shouldReturnCategoryName() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        String categoryName = "Baking";

        // Act
        ingredient.setCategoryName(categoryName);

        // Assert
        assertEquals(categoryName, ingredient.getCategoryName());
    }

    @Test
    void shouldReturnRecipe() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        Recipe recipe = new Recipe();

        // Act
        ingredient.setRecipe(recipe);

        // Assert
        assertEquals(recipe, ingredient.getRecipe());
    }

    @Test
    void shouldReturnShoppingList() {
        // Arrange
        Ingredient ingredient = new Ingredient();
        List<ShoppingList> shoppingList = new ArrayList<>();
        shoppingList.add(new ShoppingList());
        shoppingList.add(new ShoppingList());

        // Act
        ingredient.setShoppingList(shoppingList);

        // Assert
        assertEquals(shoppingList, ingredient.getShoppingList());
    }
}