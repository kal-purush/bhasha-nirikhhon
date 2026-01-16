package pro.sky.recipessite.services.impl;

import pro.sky.recipessite.model.Ingredient;
import pro.sky.recipessite.services.IngredientService;

import java.util.HashMap;
import java.util.Map;

public class IngredientServiceImpl implements IngredientService {
    private static int id = 0;
    private static Map<Integer, Ingredient> ingredients = new HashMap<>();

    @Override
    public int addIngredient(Ingredient ingredient) {
        ingredients.put(++id, ingredient);
        return id;
    }
    @Override
    public Ingredient getIngredient(int id) {
        return ingredients.get(id);
    }
}