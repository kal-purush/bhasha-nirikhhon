package com.ezgroceries.shoppinglist.service;

import com.ezgroceries.shoppinglist.internal.cocktail.CocktailEntity;
import com.ezgroceries.shoppinglist.internal.cocktail.CocktailRepository;
import com.ezgroceries.shoppinglist.search.SearchCocktailDbResponse;
import com.ezgroceries.shoppinglist.search.SearchCocktailDbResponse.DrinkResource;
import com.ezgroceries.shoppinglist.dto.CocktailResource;
import com.ezgroceries.shoppinglist.search.SearchCocktailDbClient;
import io.micrometer.core.instrument.util.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CocktailService {

    private SearchCocktailDbClient searchClient;
    private CocktailRepository cocktailRepository;

    @Autowired
    public CocktailService(SearchCocktailDbClient searchClient, CocktailRepository cocktailRepository) {
        this.searchClient = searchClient;
        this.cocktailRepository = cocktailRepository;
    }

    public List<CocktailResource> searchCocktails(String search) {
        SearchCocktailDbResponse cocktailDbResponse = searchClient.searchCocktails(search);
        List<DrinkResource> drinkResources = cocktailDbResponse.getDrinks();
        mergeCocktails(drinkResources);
        List<CocktailResource> cocktailResources = new ArrayList<>();
        for(DrinkResource drinkResource : drinkResources) {
            CocktailResource cocktailResource = new CocktailResource();
            cocktailResource.setId(UUID.randomUUID());
            cocktailResource.setDescription(drinkResource.getStrDrink());
            cocktailResource.setInstruction(drinkResource.getStrInstructions());
            cocktailResource.setUrl(drinkResource.getStrDrinkThumb());
            cocktailResource.setTypeOfGlass(drinkResource.getStrGlass());
            cocktailResource.setIngredients(getIngredients(drinkResource));
            cocktailResources.add(cocktailResource);
        }

        return cocktailResources;
    }

    public List<CocktailResource> mergeCocktails(List<SearchCocktailDbResponse.DrinkResource> drinks) {
        //Get all the idDrink attributes
        List<String> ids = drinks.stream().map(SearchCocktailDbResponse.DrinkResource::getIdDrink).collect(Collectors.toList());

        //Get all the ones we already have from our DB, use a Map for convenient lookup
        Map<String, CocktailEntity> existingEntityMap = cocktailRepository.findByIdDrinkIn(ids).stream().collect(Collectors.toMap(CocktailEntity::getIdDrink, o -> o, (o, o2) -> o));

        //Stream over all the drinks, map them to the existing ones, persist a new one if not existing
        Map<String, CocktailEntity> allEntityMap = drinks.stream().map(drinkResource -> {
            CocktailEntity cocktailEntity = existingEntityMap.get(drinkResource.getIdDrink());
            if (cocktailEntity == null) {
                CocktailEntity newCocktailEntity = new CocktailEntity();
                newCocktailEntity.setId(UUID.randomUUID());
                newCocktailEntity.setIdDrink(drinkResource.getIdDrink());
                newCocktailEntity.setName(drinkResource.getStrDrink());
                cocktailEntity = cocktailRepository.save(newCocktailEntity);
            }
            return cocktailEntity;
        }).collect(Collectors.toMap(CocktailEntity::getIdDrink, o -> o, (o, o2) -> o));

        //Merge drinks and our entities, transform to CocktailResource instances
        return mergeAndTransform(drinks, allEntityMap);
    }

    private List<CocktailResource> mergeAndTransform(List<SearchCocktailDbResponse.DrinkResource> drinks, Map<String, CocktailEntity> allEntityMap) {
        return drinks.stream().map(drinkResource -> new CocktailResource(allEntityMap.get(drinkResource.getIdDrink()).getId(), drinkResource.getStrDrink(), drinkResource.getStrGlass(),
                drinkResource.getStrInstructions(), drinkResource.getStrDrinkThumb(), getIngredients(drinkResource))).collect(Collectors.toList());
    }

    private List<String> getIngredients(DrinkResource drinkResource) {
        // Why did db cocktail remote not return a list of ordered ingredients :-)
        // Alternative is using reflection, but for now old school.
        List<String> ingredients = new ArrayList<>();
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient1());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient2());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient3());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient4());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient5());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient6());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient7());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient8());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient9());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient10());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient11());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient12());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient13());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient14());
        addIngredientIfNotEmpty(ingredients, drinkResource.getStrIngredient15());
        return ingredients;
    }

    private void addIngredientIfNotEmpty(List<String> ingredients, String ingredient) {
        if(StringUtils.isNotEmpty(ingredient)) {
            ingredients.add(ingredient);
        }
    }

}