package com.group4sweng.scranplan.MealPlanner.Ingredients;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Ingredients Class JUnit tests.
 *  Author: JButler
 *  (c) CoDev 2020
 */
public class IngredientTest {

    //  Test variables.
    private final String NAME = "Celery";
    private final String PORTION = "2 sticks";
    private final int FAKE_ICON = 12345; // Fake icon number. (Unit tests don't include Android Services for Drawables).
    private final String WARNING = "Portions estimates are different for Alcohol and Herbs";

    //  Test a basic ingredient can be created.
    @Test
    public void testCreateSimpleIngredient() {
        Ingredient ingredient = new Ingredient(NAME, PORTION);

        assertEquals(ingredient.getName(), NAME);
        assertEquals(ingredient.getPortion(), PORTION);
    }

    //  Test an ingredient intended for the Mealplanner can be created.
    @Test
    public void testCreateMealplannerIngredient() {
        Ingredient ingredient = new Ingredient(NAME, PORTION, FAKE_ICON, WARNING);

        assertEquals(ingredient.getName(), NAME);
        assertEquals(ingredient.getPortion(), PORTION);
        assertEquals(ingredient.getIcon(), 12345);
        assertEquals(ingredient.getWarning(), WARNING);
    }

    //  Test we can set ingredients properly.
    @Test
    public void testSetIngredient() {
        Ingredient ingredient = new Ingredient(NAME, PORTION);

        ingredient.setIcon(FAKE_ICON);
        ingredient.setName(NAME);
        ingredient.setPortion(PORTION);
        ingredient.setWarning(WARNING);

        assertEquals(ingredient.getName(), NAME);
        assertEquals(ingredient.getPortion(), PORTION);
        assertEquals(ingredient.getIcon(), 12345);
        assertEquals(ingredient.getWarning(), WARNING);
    }
}