package com.ezgroceries.shoppinglist.search;

import com.ezgroceries.shoppinglist.search.SearchCocktailDbResponse.DrinkResource;
import io.micrometer.core.instrument.util.StringUtils;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.springframework.util.ReflectionUtils;

public class IngredientUtils {

    private final static int MAX_INGREDIENTS = 15;

    private IngredientUtils() {
        super();
    }

    public static void addIngredientsTo(DrinkResource drinkResource, Set<String> ingredients) {
        if(ingredients != null) {
            int ingredientCounter = 0;
            for(String ingredient : ingredients) {
                Method ingredientSetter = ReflectionUtils.findMethod(SearchCocktailDbResponse.DrinkResource.class,
                        String.format("setStrIngredient%d", ++ingredientCounter), String.class);
                if(ingredientSetter != null) {
                    ReflectionUtils.invokeMethod(ingredientSetter, drinkResource, ingredient);
                }
            }
        }
    }

    public static List<String> getIngredientsFrom(DrinkResource drinkResource) {
        List<String> ingredients = new ArrayList<>();
        if (drinkResource != null) {
            for (int i = 0; i < MAX_INGREDIENTS; i++) {
                Method ingredientGetter = ReflectionUtils.findMethod(SearchCocktailDbResponse.DrinkResource.class,
                        String.format("getStrIngredient%d", i + 1));
                if (ingredientGetter != null) {
                    String ingredient = (String) ReflectionUtils.invokeMethod(ingredientGetter, drinkResource);
                    if (StringUtils.isNotEmpty(ingredient)) {
                        ingredients.add(ingredient);
                    }
                }
            }
        }

        return ingredients;
    }

}