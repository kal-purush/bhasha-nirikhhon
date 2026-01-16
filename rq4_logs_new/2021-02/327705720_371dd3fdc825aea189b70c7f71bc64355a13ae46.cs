using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BackendClassLibrary
{
  public interface IDatabaseManager
  {
    //Creates then runs a delete query
    bool deleteDrink(Drink drink);

    //Creates then runs an insert query
    bool addDrink(Drink drink);

    //Creates then runs an update query
    bool updateDrink(Drink drink);

    //Querys the database for all entries in the drinks table and returns them as a list
    List<Item> getDrinks();

    //Creates then runs a delete query for entry IN RECIPES TABLE ONLY
    //NOTE: DO NOT ATTEMPT TO USE THIS BEFORE DELETING ALL CONNECTED ENTRIES IN DRINKRECIPE TABLE 
    bool deleteRecipe(int recipeID);

    //Deletes all entries in the drinkRecipes Table with a certain recipeID
    bool deleteDrinkRecipesByRecipeID(int recipeID);

    //Creates then runs an insert query FOR JUST THE RECIPES TABLE
    //returns the ID of the newly Inserted Recipe
    int addRecipe(string name, string baseLiquor);

    //Creates then runs an insert query FOR JUST THE DRINKRECIPES TABLE
    //ASSUMES THAT RECIPE AND DRINK IDS ARE VALID. AKA THAT THEY BELONG TO DRINKS AND RECIPES THAT EXIST 
    int addDrinkRecipe(int recipeID, int drinkID, double itemQuantity);

    /* IMPORTANT!: DrinkRecipe Table Updates for am existing Recipe will be handled by Deleting and 
    all DrinkRecipe Entries related to that table and re-adding/replacing the entries with entire ItemList
    of the updated Recipe Object, for Simplicity's sake. */

    // Querys the database for all entries in the drinkrecipes, and the pertinent information  
    // of their respectively referenced Recipe and Drink entries
    List<Recipe> getRecipes();
  }
    
}