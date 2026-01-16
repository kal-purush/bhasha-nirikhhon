import util.Input;

import java.util.ArrayList;
import java.util.HashMap;

public class GroceryList {
    public ArrayList<GroceryItem> aisleItems = new ArrayList<>();
    public ArrayList<GroceryItem> bakedGoods = new ArrayList<>();
    public ArrayList<GroceryItem> dairyGoods = new ArrayList<>();
    public ArrayList<GroceryItem> fruitsAndVeggies = new ArrayList<>();
    public ArrayList<GroceryItem> animalProteins = new ArrayList<>();


    /*
    * TODO:
    *  A user should be prompted if they would like to create a grocery list.
        yes? they will be prompted if they would like to enter a new item.
            yes? they should be given the following three prompts:
                Given an ordered list of grocery categories to choose from, select the category.
                Enter the name of the item.
                Enter how many of the item.
        The user will then be given the choice to finalize the list or add an item.
        Once the user finalizes the list, they will be presented with a complete list
        * organized alphabetically and
        * grouped by a category, and
        * including quantity.
    */
    public static void main(String[] args) {
        Input choice = new Input();
        System.out.println("Would you like to start a grocery list?");
        if(choice.yesNo()){
            createBlankList();

        }
    }

    public static HashMap createBlankList(){
        HashMap<String, ArrayList> groceryList = new HashMap<>();
        groceryList.put("Aisle", aisleItems)
    }

}