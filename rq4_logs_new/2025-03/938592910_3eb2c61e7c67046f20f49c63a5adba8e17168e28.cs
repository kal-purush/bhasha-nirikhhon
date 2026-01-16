using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Crafter : MonoBehaviour
{
    [Header("Crafting Settings")]
    public InventorySystem playerInventory;  // Reference to player's inventory
    public List<CraftingRecipe> availableRecipes;  // List of available crafting recipes
    public GameObject craftingUI;  // UI panel to show crafting options

    [Header("UI Elements")]
    public Text recipeNameText;  // To show selected recipe name
    public Text requiredItemsText;  // To show required items for the selected recipe
    public Button craftButton;  // Button to craft the item

    private CraftingRecipe selectedRecipe;  // Currently selected recipe

    void Start()
    {
        // Hide the crafting UI by default
        craftingUI.SetActive(false);

        // Initialize buttons
        craftButton.onClick.AddListener(OnCraftButtonClicked);
    }

    // Call this method to open the crafting UI and display recipes
    public void OpenCraftingUI()
    {
        craftingUI.SetActive(true);
        DisplayAvailableRecipes();
    }

    // Close the crafting UI
    public void CloseCraftingUI()
    {
        craftingUI.SetActive(false);
    }

    // Display available recipes in the UI (this could be buttons or text)
    void DisplayAvailableRecipes()
    {
        foreach (var recipe in availableRecipes)
        {
            // Buttons, each with the recipe name
            Button recipeButton = CreateRecipeButton(recipe);
            recipeButton.onClick.AddListener(() => SelectRecipe(recipe));
        }
    }

    // Create a UI button for each recipe
    Button CreateRecipeButton(CraftingRecipe recipe)
    {
        GameObject buttonObj = new GameObject(recipe.itemName);
        buttonObj.transform.SetParent(craftingUI.transform);  // Parent the button to UI

        Button recipeButton = buttonObj.AddComponent<Button>();
        Text buttonText = buttonObj.AddComponent<Text>();
        buttonText.text = recipe.itemName;

        // You can also adjust the button's look here if needed

        return recipeButton;
    }

    // Select a recipe when clicked
    void SelectRecipe(CraftingRecipe recipe)
    {
        selectedRecipe = recipe;
        recipeNameText.text = "Crafting: " + recipe.itemName;
        requiredItemsText.text = "Required: " + GetRequiredItemsText(recipe);
    }

    // Get the list of required items for the selected recipe
    string GetRequiredItemsText(CraftingRecipe recipe)
    {
        string requiredItems = "";
        foreach (var item in recipe.requiredItems)
        {
            requiredItems += item.Key + " x" + item.Value + "\n";
        }
        return requiredItems;
    }

    // This method is called when the craft button is clicked
    void OnCraftButtonClicked()
    {
        if (selectedRecipe != null)
        {
            if (selectedRecipe.CanCraft(playerInventory.GetInventory())) // Check if the player has enough items
            {
                CraftItem(selectedRecipe);
            }
            else
            {
                Debug.Log("Not enough materials to craft: " + selectedRecipe.itemName);
            }
        }
    }

    // Craft the selected item and update the inventory
    void CraftItem(CraftingRecipe recipe)
    {
        // Remove the required items from the inventory
        foreach (var item in recipe.requiredItems)
        {
            playerInventory.RemoveItem(item.Key, item.Value);
        }

        // Add the crafted item to the inventory
        playerInventory.AddItem(recipe.itemPrefab, recipe.itemName);

        // Close the crafting UI after crafting
        CloseCraftingUI();

        Debug.Log("Crafted " + recipe.itemName);
    }
}