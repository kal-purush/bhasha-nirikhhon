package com.ezgroceries.shoppinglist;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ezgroceries.shoppinglist.dto.CocktailReference;
import com.ezgroceries.shoppinglist.dto.ShoppingListResource;
import com.ezgroceries.shoppinglist.internal.cocktail.CocktailEntity;
import com.ezgroceries.shoppinglist.internal.cocktail.CocktailRepository;
import com.ezgroceries.shoppinglist.internal.shoppinglist.ShoppingListEntity;
import com.ezgroceries.shoppinglist.internal.shoppinglist.ShoppingListRepository;
import com.ezgroceries.shoppinglist.service.ShoppingListService;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class ShoppingListServiceTest {

    private final static UUID FIXED_SHOPPING_LIST_ID = UUID.randomUUID();
    private final static String FIXED_SHOPPING_LIST_NAME = "myShoppingList";

    private final static String FIXED_COCKTAIL_DRINK_ID = UUID.randomUUID().toString();
    private final static UUID FIXED_COCKTAIL_ID = UUID.randomUUID();
    private final static String FIXED_COCKTAIL_NAME = "Margarita";
    private final static Set<String> FIXED_COCKTAIL_INGREDIENTS = Stream.of("Vodka", "Tequila").collect(Collectors.toSet());
    private final static UUID FIXED_UNKNOWN_SHOPPING_LIST_ID = UUID.randomUUID();
    private final static UUID FIXED_UNKNOWN_COCKTAIL_ID = UUID.randomUUID();

    private ShoppingListRepository shoppingListRepository;
    private CocktailRepository cocktailRepository;
    private ShoppingListEntity shoppingListEntity;

    @Before
    public void prepareMocking() {
        shoppingListRepository = mock(ShoppingListRepository.class);
        cocktailRepository = mock(CocktailRepository.class);

        CocktailEntity cocktailEntity = new CocktailEntity();
        cocktailEntity.setIngredients(FIXED_COCKTAIL_INGREDIENTS);
        cocktailEntity.setIdDrink(FIXED_COCKTAIL_DRINK_ID);
        cocktailEntity.setId(FIXED_COCKTAIL_ID);
        cocktailEntity.setName(FIXED_COCKTAIL_NAME);

        shoppingListEntity = new ShoppingListEntity();
        shoppingListEntity.setId(FIXED_SHOPPING_LIST_ID);
        shoppingListEntity.setName(FIXED_SHOPPING_LIST_NAME);
        shoppingListEntity.setCocktails(Stream.of(cocktailEntity).collect(Collectors.toSet()));
        cocktailEntity.setShoppingLists(Stream.of(shoppingListEntity).collect(Collectors.toSet()));

        when(shoppingListRepository.save(any())).thenReturn(shoppingListEntity);
        when(shoppingListRepository.findById(ArgumentMatchers.eq(FIXED_SHOPPING_LIST_ID)))
                .thenReturn(Optional.of(shoppingListEntity));
        when(shoppingListRepository.findById(ArgumentMatchers.eq(FIXED_UNKNOWN_SHOPPING_LIST_ID)))
                .thenReturn(Optional.empty());
        when(shoppingListRepository.findAll())
                .thenReturn(Arrays.asList(shoppingListEntity));

        when(cocktailRepository.findById(ArgumentMatchers.eq(FIXED_COCKTAIL_ID)))
                .thenReturn(Optional.of(cocktailEntity));
        when(cocktailRepository.findById(ArgumentMatchers.eq(FIXED_UNKNOWN_COCKTAIL_ID)))
                .thenReturn(Optional.empty());
    }

    @Test
    public void testCreate() {
        // GIVEN
        ShoppingListService shoppingListService = new ShoppingListService(shoppingListRepository, cocktailRepository);

        // WHEN
        ShoppingListResource shoppingListResource = shoppingListService.create(FIXED_SHOPPING_LIST_NAME);

        // THEN
        Assert.assertEquals(FIXED_SHOPPING_LIST_NAME, shoppingListResource.getName());
        Assert.assertEquals(FIXED_SHOPPING_LIST_ID, shoppingListResource.getId());
        Assert.assertNull(shoppingListResource.getIngredients());
    }

    @Test
    public void testGetList() {
        // GIVEN
        ShoppingListService shoppingListService = new ShoppingListService(shoppingListRepository, cocktailRepository);

        // WHEN
        ShoppingListResource shoppingListResource = shoppingListService.get(FIXED_SHOPPING_LIST_ID);

        // THEN
        Assert.assertNotNull(shoppingListResource);
        Assert.assertEquals(FIXED_SHOPPING_LIST_NAME, shoppingListResource.getName());
        Assert.assertEquals(FIXED_SHOPPING_LIST_ID, shoppingListResource.getId());
        Assert.assertTrue(FIXED_COCKTAIL_INGREDIENTS.equals(new HashSet<>(shoppingListResource.getIngredients())));
    }

    @Test
    public void testGetMissingList() {
        // GIVEN
        ShoppingListService shoppingListService = new ShoppingListService(shoppingListRepository, cocktailRepository);

        // WHEN
        ShoppingListResource shoppingListResource = shoppingListService.get(FIXED_UNKNOWN_SHOPPING_LIST_ID);

        // THEN
        Assert.assertNull(shoppingListResource);
    }

    @Test
    public void testGetAll() {
        // GIVEN
        ShoppingListService shoppingListService = new ShoppingListService(shoppingListRepository, cocktailRepository);

        // WHEN
        List<ShoppingListResource> shoppingListResources = shoppingListService.getAll();

        // THEN
        Assert.assertNotNull(shoppingListResources);
        Assert.assertTrue(shoppingListResources.size() == 1);
        Assert.assertEquals(shoppingListResources.get(0).getId(), FIXED_SHOPPING_LIST_ID);
    }

    @Test
    public void testAddCocktail() {
        // GIVEN
        ShoppingListService shoppingListService = new ShoppingListService(shoppingListRepository, cocktailRepository);
        CocktailReference cocktailReference = new CocktailReference();
        cocktailReference.setCocktailId(FIXED_COCKTAIL_ID);
        List<CocktailReference> cocktailReferences = Arrays.asList(cocktailReference);


        // WHEN
        List<CocktailReference> addedCocktailReferences = shoppingListService
                .addCocktails(FIXED_SHOPPING_LIST_ID, cocktailReferences);

        // THEN
        Assert.assertNotNull(addedCocktailReferences);
        Assert.assertTrue(cocktailReferences.equals(addedCocktailReferences));
    }

    @Test
    public void testAddMissingCocktail() {
        // GIVEN
        ShoppingListService shoppingListService = new ShoppingListService(shoppingListRepository, cocktailRepository);
        CocktailReference cocktailReference = new CocktailReference();
        cocktailReference.setCocktailId(FIXED_UNKNOWN_COCKTAIL_ID);
        List<CocktailReference> cocktailReferences = Arrays.asList(cocktailReference);

        // WHEN
        List<CocktailReference> addedCocktailReferences = shoppingListService
                .addCocktails(FIXED_SHOPPING_LIST_ID, cocktailReferences);

        // THEN
        Assert.assertNotNull(addedCocktailReferences);
        Assert.assertTrue(addedCocktailReferences.isEmpty());
    }

}