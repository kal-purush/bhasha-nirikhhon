using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GhostTrading : MonoBehaviour
{
    public int maxIngredientsCount = 1;
    public IngredientManager ingredientManager;
    public RecipeDatabase recipeDatabase;

    private Ingredient.Name currentIngredient = new Ingredient.Name();

    private void OnTriggerEnter(Collider other)
    {
        Ingredient ingredient = other.GetComponent<Ingredient>();
        if (ingredient != null)
        {
            currentIngredient = ingredient.name;



            switch (currentIngredient.ToString())
            {
                case "Cake":
                case "Cupcake":
                case "Donut":
                    Destroy(other.gameObject);
                    CheckTrade();
                    StartCoroutine(RespawnIngredientsAfterFrame());
                    break;
                default:
                    break;
            }
        }
        else
        {
            Rigidbody rb = other.GetComponent<Rigidbody>();
            if (rb != null)
            {
                rb.AddForce(-other.transform.forward * 1, ForceMode.Impulse);
            }
        }
    }
    private IEnumerator RespawnIngredientsAfterFrame()
    {
        yield return new WaitForEndOfFrame();
        ingredientManager.RespawnAllIngredients();
    }

    private void CheckTrade()
    {
        foreach (var recipe in recipeDatabase.recipes)
        {
            if (IsTradeCorrect(recipe))
            {
                SpawnNewIngredient(recipe.resultObjectPrefab.GetComponent<Ingredient>().name);
                return;
            }
        }
    }

    private bool IsTradeCorrect(Recipe recipe)
    {
        if (recipe.ingredients.Count != 1)
        {
            return false;
        }

        Ingredient.Name tempIngredients = currentIngredient;

        foreach (var ingredient in recipe.ingredients)
        {
            if (tempIngredients == ingredient)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        return false;
    }
    private void SpawnNewIngredient(Ingredient.Name newIngredient)
    {
        ingredientManager.UnlockIngredient(newIngredient);
    }
}