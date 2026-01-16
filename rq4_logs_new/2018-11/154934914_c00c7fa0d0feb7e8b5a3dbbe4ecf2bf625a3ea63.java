package com.kudoji.taco.data;

import com.kudoji.taco.Ingredient;

public interface IngredientRepository {
    Iterable<Ingredient> findAll();

    Ingredient findById(String id);

    boolean save(Ingredient ingredient);
}