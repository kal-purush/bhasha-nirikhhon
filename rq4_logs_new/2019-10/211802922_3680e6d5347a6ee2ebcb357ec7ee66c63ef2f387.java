package com.ezgroceries.shoppinglist.service;

import com.ezgroceries.shoppinglist.contract.CocktailReference;
import com.ezgroceries.shoppinglist.contract.ShoppingListResource;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class ShoppingListService {

    // todo replace with crud repositories

    public ShoppingListResource create(String listName) {
        return new ShoppingListResource(
                UUID.randomUUID(),
                listName
        );
    }

    public ShoppingListResource get(UUID listId) {
        return new ShoppingListResource(
                UUID.fromString(listId.toString()), "My shop list",
                Arrays.asList("Tequila", "Triple sec", "Lime juice", "Salt"));
    }

    public List<CocktailReference> addCocktails(UUID listId, List<CocktailReference> cocktailIds) {
        CocktailReference addedCocktailReference = new CocktailReference();
        addedCocktailReference.setCocktailId(cocktailIds.get(0).getCocktailId());
        return Arrays.asList(addedCocktailReference);
    }

    public List<ShoppingListResource> lists() {
        return Arrays.asList(
                get(UUID.randomUUID()),
                get(UUID.randomUUID()),
                get(UUID.randomUUID())
        );
    }

}