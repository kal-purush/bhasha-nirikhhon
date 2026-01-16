package com.kitchen.iChef.Service;

import com.kitchen.iChef.DTO.RecipeDTO;
import com.kitchen.iChef.DTO.RecipeIngredientDTO;
import com.kitchen.iChef.DTO.RecipeUtensilDTO;
import com.kitchen.iChef.DTO.UpdateRecipeDTO;
import com.kitchen.iChef.Domain.AppUser;
import com.kitchen.iChef.Domain.Ingredient;
import com.kitchen.iChef.Domain.Recipe;
import com.kitchen.iChef.Domain.RecipeIngredient;
import com.kitchen.iChef.Mapper.RecipeIngredientMapper;
import com.kitchen.iChef.Mapper.RecipeMapper;
import com.kitchen.iChef.Repository.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(MockitoExtension.class)
class RecipeServiceTest {
    @InjectMocks
    RecipeService recipeService;
    @Mock
    RecipeRepository recipeRepository;
    @Mock
    IngredientRepository ingredientRepository;
    @Mock
    UtensilRepository utensilRepository;
    @Mock
    RecipeIngredientRepository recipeIngredientRepository;
    @Mock
    RecipeUtensilRepository recipeUtensilRepository;
    @Mock
    UserRepository userRepository;
    @Mock
    RecipeIngredientMapper recipeIngredientMapper;
    @Mock
    RecipeMapper recipeMapper;



    private Recipe createRecipe(String recipeId, String title, String steps, Float rating, Float difficulty,
                                      Integer preparationTime, Integer portions, String notes,
                                      Integer numberOfViews, String imagePath, AppUser appUser){
        Recipe recipe = new Recipe();
        recipe.setRecipeId(recipeId);
        recipe.setTitle(title);
        recipe.setSteps(steps);
        recipe.setRating(rating);
        recipe.setDifficulty(difficulty);
        recipe.setPreparationTime(preparationTime);
        recipe.setPortions(portions);
        recipe.setNotes(notes);
        recipe.setNumberOfViews(numberOfViews);
        recipe.setImagePath(imagePath);
        recipe.setAppUser(appUser);
        return recipe;
    }

    private AppUser createUser(String userId, String firstName,String lastName, String username, String email, ZonedDateTime joinedDate, ZonedDateTime lastOnline, Boolean isAdmin, String hashedPassword)
    {
        AppUser appUser = new AppUser();
        appUser.setUserId(userId);
        appUser.setFirstName(firstName);
        appUser.setLastName(lastName);
        appUser.setUsername(username);
        appUser.setEmail(email);
        appUser.setJoinedDate(joinedDate);
        appUser.setLastOnline(lastOnline);
        appUser.setIsAdmin(isAdmin);
        appUser.setHashedPassword(hashedPassword);
        return appUser;
    }

    private RecipeDTO createRecipeDTO(String recipeId, String title, String steps, Float rating, Float difficulty,
                                      Integer preparationTime, Integer portions, String notes,
                                      Integer numberOfViews, String imagePath, String userId,
                                      List<RecipeIngredientDTO> recipeIngredientDTOSList,
                                      List<RecipeUtensilDTO> recipeUtensilDTOSList)
    {
        RecipeDTO recipeDTO = new RecipeDTO();
        recipeDTO.setRecipeId(recipeId);
        recipeDTO.setTitle(title);
        recipeDTO.setSteps(steps);
        recipeDTO.setRating(rating);
        recipeDTO.setDifficulty(difficulty);
        recipeDTO.setPreparationTime(preparationTime);
        recipeDTO.setPortions(portions);
        recipeDTO.setNotes(notes);
        recipeDTO.setNumberOfViews(numberOfViews);
        recipeDTO.setImagePath(imagePath);
        recipeDTO.setUserId(userId);
        recipeDTO.setRecipeIngredientDTOSList(recipeIngredientDTOSList);
        recipeDTO.setRecipeUtensilDTOSList(recipeUtensilDTOSList);
        return recipeDTO;
    }

    private UpdateRecipeDTO createUpdateRecipeDTO(String recipeId, String title, String steps, Float rating, Float difficulty,
                                                  Integer preparationTime, Integer portions, String notes,
                                                  Integer numberOfViews, String imagePath,
                                                  List<RecipeIngredientDTO> recipeIngredientDTOSList,
                                                  List<RecipeUtensilDTO> recipeUtensilDTOSList){
        UpdateRecipeDTO updateRecipeDTO = new UpdateRecipeDTO();
        updateRecipeDTO.setRecipeId(recipeId);
        updateRecipeDTO.setTitle(title);
        updateRecipeDTO.setSteps(steps);
        updateRecipeDTO.setRating(rating);
        updateRecipeDTO.setDifficulty(difficulty);
        updateRecipeDTO.setPreparationTime(preparationTime);
        updateRecipeDTO.setPortions(portions);
        updateRecipeDTO.setNotes(notes);
        updateRecipeDTO.setNumberOfViews(numberOfViews);
        updateRecipeDTO.setImagePath(imagePath);
        updateRecipeDTO.setRecipeIngredientDTOSList(recipeIngredientDTOSList);
        updateRecipeDTO.setRecipeUtensilDTOSList(recipeUtensilDTOSList);
        return updateRecipeDTO;
    }

    @Test
    @DisplayName("Test Add Success Case")
    void add_success() {
        List<RecipeIngredientDTO> list1 = new ArrayList<>();
        List<RecipeUtensilDTO> list2 = new ArrayList<>();
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        RecipeDTO recipeDTO = createRecipeDTO("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", "1",list1,list2);
        Recipe recipe = createRecipe("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", appUser);
        Mockito.when(userRepository.findOne("1")).thenReturn(appUser);
        recipeService.addRecipe(recipeDTO);
        recipe.setAppUser(appUser);
        Mockito.verify(recipeRepository).save(any());
    }

    @Test
    @DisplayName("Test Add Failure Case")
    void add_failure(){
        List<RecipeIngredientDTO> list1 = new ArrayList<>();
        List<RecipeUtensilDTO> list2 = new ArrayList<>();
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        RecipeDTO recipeDTO = createRecipeDTO("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", "userid",list1,list2);
        Recipe recipe = createRecipe("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", appUser);
        Mockito.doThrow(new RuntimeException("Testing Failure Case" )).
                when(recipeRepository).
                save(recipe);
        Assertions.assertThrows(Exception.class, () -> {
            recipeService.addRecipe(recipeDTO);
        });
    }

    @Test
    @DisplayName("Test GetAll Success Case")
    void getAll_success() {
        List<Recipe> list = new ArrayList<>();
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        list.add(createRecipe("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", appUser));
        list.add(createRecipe("10","Pizza","steps",9.2f,4.2f,50,2,"notes",240,"path2.png", appUser));
        Mockito.when(recipeRepository.findAll()).thenReturn(list);
        List<RecipeDTO> recipes = recipeService.getAllRecipes();
        Mockito.verify(recipeRepository).findAll();

        Assertions.assertEquals(2,recipes.size());
        Assertions.assertEquals("Paste",recipes.get(0).getTitle());
        Assertions.assertEquals("Pizza",recipes.get(1).getTitle());
    }

    @Test
    @DisplayName("Test GetAll Failure Case")
    void getAll_failure() {
        Mockito.doThrow(new RuntimeException("Testing Failure Case"))
                .when(recipeRepository)
                .findAll();
        Assertions.assertThrows(Exception.class, () -> {
            recipeService.getAllRecipes();
        });
    }

    @Test
    @DisplayName("Test Delete Success Case")
    void delete_success() {
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        Recipe recipe = createRecipe("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", appUser);
        Mockito.when(recipeRepository.findOne("1")).thenReturn(recipe);
        Mockito.when(recipeRepository.delete("1")).thenReturn(recipe);
        recipeService.deleteRecipe("1");
        Mockito.verify(recipeRepository).delete("1");
    }

    @Test
    @DisplayName("Test Delete Failure Case")
    void delete_failure() {
        Mockito.doThrow(new RuntimeException("Testing Failure Case"))
                .when(recipeRepository)
                .delete("1");
        Assertions.assertThrows(Exception.class, () -> {
            recipeRepository.delete("1");
        });
    }

    @Test
    @DisplayName(("Test Get Recipe Case"))
    void getRecipe(){
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        Recipe recipe = createRecipe("1","Paste","steps",9.5f,4f,40,5,"notes",70,"path.png", appUser);
        Mockito.when(recipeRepository.findOne("1")).thenReturn(recipe);

        recipeService.getRecipe("1");
        Mockito.verify(recipeRepository).findOne("1");
    }

    @Test
    @DisplayName(("Test Simple Recipe Filtering Case"))
    void simpleRecipeFiltering(){
        List<Recipe> list = new ArrayList<>();
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        list.add(createRecipe("1","Pizza Margherita","steps",9.5f,2f,40,6,"notes",70,"path.png", appUser));
        list.add(createRecipe("6","Pizza Prosciutto","steps",8.3f,2.8f,50,4,"notes",54,"path2.png", appUser));
        Mockito.when(recipeRepository.findRecipesByTitle("Pizza")).thenReturn(list);
        List<RecipeDTO> recipeDTOS = recipeService.simpleRecipeFiltering("Pizza");
        Mockito.verify(recipeRepository).findRecipesByTitle("Pizza");

        Assertions.assertEquals(2,recipeDTOS.size());
        Assertions.assertTrue(recipeDTOS.get(0).getTitle().contains("Pizza"));
        Assertions.assertTrue(recipeDTOS.get(1).getTitle().contains("Pizza"));
    }

    @Test
    @DisplayName(("Test Get All User Recipes Case"))
    void getAllUserRecipes(){
        List<Recipe> list = new ArrayList<>();
        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        list.add(createRecipe("1","Pizza","steps",9.35f,2f,40,6,"notes",70,"path.png", appUser));
        list.add(createRecipe("6","Tiramisu","steps",9.8f,4.2f,80,20,"notes",137,"path2.png", appUser));
        Mockito.when(recipeRepository.findAll()).thenReturn(list);
        List<RecipeDTO> recipeDTOS = recipeService.getAllUserRecipes("1");
        Mockito.verify(recipeRepository).findAll();

        Assertions.assertEquals(2,recipeDTOS.size());
        Assertions.assertTrue(recipeDTOS.get(0).getTitle().contains("Pizza"));
        Assertions.assertTrue(recipeDTOS.get(1).getTitle().contains("Tiramisu"));
    }

    @Test
    @DisplayName(("Test Update Recipe Case"))
    void updateRecipe(){
        List<RecipeIngredientDTO> list1 = new ArrayList<>();
        List<RecipeUtensilDTO> list2 = new ArrayList<>();
        UpdateRecipeDTO updateRecipeDTO = createUpdateRecipeDTO("1","Cheeseburger","steps",6.7f,6.7f,50,2,"notes",50,"path.jpg",list1,list2);

        AppUser appUser = createUser("1","Andrei", "Pop", "andrei.pop","andreipop@gmail.com",ZonedDateTime.now(),ZonedDateTime.now(),false,"popcorn");
        Recipe recipe = createRecipe("1","Cheeseburger","steps",6.7f,6.7f,50,2,"notes",50,"path.jpg", appUser);

        recipeService.updateRecipe(updateRecipeDTO);
        Mockito.verify(recipeRepository).update(any());
    }

}