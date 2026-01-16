import { bench } from "vitest";
import { Filters } from "./src/models/filters.ts";

import { Recipe } from "./src/models/recipe.ts";

import jsonData from "./data/recipes.json";

const allRecipes: Recipe[] = jsonData.recipes;
const searchInput = "tomate";

//With no filters selected
bench(
  "array algo",
  () => {
    let recipesDisplayed: Recipe[];
    recipesDisplayed = allRecipes.filter((recipe) => {
      const textMatch = matchText(recipe, searchInput);

      return textMatch;
    });

    function matchText(recipe: Recipe, searchTerm: string): boolean {
      if (searchTerm.length < 3) {
        return true;
      }
      searchTerm = searchTerm.toLowerCase();
      const nameMatch = recipe.name.toLowerCase().includes(searchTerm);
      const descriptionMatch = recipe.description
        .toLowerCase()
        .includes(searchTerm);
      const ingredientMatch = recipe.ingredients.some((ingredient) =>
        ingredient.ingredient.toLowerCase().includes(searchTerm)
      );

      return nameMatch || descriptionMatch || ingredientMatch;
    }
  },
  { time: 5_000 }
);

bench(
  "for loop algo",
  () => {
    let recipesDisplayed = [];

    for (let i = 0; i < allRecipes.length; i++) {
      let recipe = allRecipes[i];

      if (matchText(recipe, searchInput)) {
        recipesDisplayed.push(recipe);
      }
    }

    function matchText(recipe: Recipe, searchTerm: string): boolean {
      if (searchTerm.length < 3) {
        return true;
      }
      searchTerm = searchTerm.toLowerCase();
      const nameMatch = recipe.name.toLowerCase().includes(searchTerm);
      const descriptionMatch = recipe.description
        .toLowerCase()
        .includes(searchTerm);
      const ingredientMatch = recipe.ingredients.some((ingredient) =>
        ingredient.ingredient.toLowerCase().includes(searchTerm)
      );

      return nameMatch || descriptionMatch || ingredientMatch;
    }
  },
  { time: 5_000 }
);

//With at least one filter selected
let selectedFilters: Filters = {
  ingredients: new Set(),
  appliances: new Set(),
  ustensils: new Set("couteau"),
};

const ustensilFilter = selectedFilters.ustensils;

bench(
  "array algo with filter",
  () => {
    let recipesDisplayed: Recipe[];
    recipesDisplayed = allRecipes.filter((recipe) => {
      const ingredientsMatch = matchIngredients(
        recipe.ingredients,
        selectedFilters.ingredients
      );
      const appliancesMatch = matchAppliances(
        recipe.appliance,
        selectedFilters.appliances
      );
      const ustensilsMatch = matchUstensils(
        recipe.ustensils,
        selectedFilters.ustensils
      );

      const textMatch = matchText(recipe, searchInput);

      return ingredientsMatch && appliancesMatch && ustensilsMatch && textMatch;
    });

    function matchIngredients(
      ingredients: { ingredient: string }[],
      filterIngredients: Set<string>
    ): boolean {
      if (filterIngredients.size === 0) {
        return true;
      }

      const recipeIngredientSet = new Set(
        ingredients.map((ingredient) => ingredient.ingredient.toLowerCase())
      );

      // Check if the recipe contains all of the selected ingredients
      return Array.from(filterIngredients).every((filter) =>
        recipeIngredientSet.has(filter)
      );
    }

    function matchAppliances(
      appliance: string,
      filterAppliances: Set<string>
    ): boolean {
      if (filterAppliances.size === 0) {
        return true;
      }

      const applianceName = appliance.toLowerCase();

      return filterAppliances.has(applianceName);
    }

    function matchUstensils(
      ustensils: string[],
      filterUstensils: Set<string>
    ): boolean {
      if (filterUstensils.size === 0) {
        return true;
      }

      const recipeUstensilSet = new Set(
        ustensils.map((ustensil) => ustensil.toLowerCase())
      );

      // Check if the recipe contains all of the selected ustensils
      return Array.from(filterUstensils).every((filter) =>
        recipeUstensilSet.has(filter)
      );
    }

    function matchText(recipe: Recipe, searchTerm: string): boolean {
      if (searchTerm.length < 3) {
        return true;
      }
      searchTerm = searchTerm.toLowerCase();
      const nameMatch = recipe.name.toLowerCase().includes(searchTerm);
      const descriptionMatch = recipe.description
        .toLowerCase()
        .includes(searchTerm);
      const ingredientMatch = recipe.ingredients.some((ingredient) =>
        ingredient.ingredient.toLowerCase().includes(searchTerm)
      );

      return nameMatch || descriptionMatch || ingredientMatch;
    }
  },
  { time: 5_000 }
);

bench(
  "for loop algo with filter",
  () => {
    let recipesDisplayed = [];

    for (let i = 0; i < allRecipes.length; i++) {
      const recipe = allRecipes[i];

      if (
        matchIngredients(recipe.ingredients, selectedFilters.ingredients) &&
        matchAppliances(recipe.appliance, selectedFilters.appliances) &&
        matchUstensils(recipe.ustensils, selectedFilters.ustensils) &&
        matchText(recipe, searchInput)
      ) {
        recipesDisplayed.push(recipe);
      }
    }

    function matchIngredients(
      ingredients: { ingredient: string }[],
      filterIngredients: Set<string>
    ): boolean {
      if (filterIngredients.size === 0) {
        return true;
      }

      const recipeIngredientSet = new Set(
        ingredients.map((ingredient) => ingredient.ingredient.toLowerCase())
      );

      // Check if the recipe contains all of the selected ingredients
      return Array.from(filterIngredients).every((filter) =>
        recipeIngredientSet.has(filter)
      );
    }

    function matchAppliances(
      appliance: string,
      filterAppliances: Set<string>
    ): boolean {
      if (filterAppliances.size === 0) {
        return true;
      }

      const applianceName = appliance.toLowerCase();

      return filterAppliances.has(applianceName);
    }

    function matchUstensils(
      ustensils: string[],
      filterUstensils: Set<string>
    ): boolean {
      if (filterUstensils.size === 0) {
        return true;
      }

      const recipeUstensilSet = new Set(
        ustensils.map((ustensil) => ustensil.toLowerCase())
      );

      // Check if the recipe contains all of the selected ustensils
      return Array.from(filterUstensils).every((filter) =>
        recipeUstensilSet.has(filter)
      );
    }

    function matchText(recipe: Recipe, searchTerm: string): boolean {
      if (searchTerm.length < 3) {
        return true;
      }
      searchTerm = searchTerm.toLowerCase();
      const nameMatch = recipe.name.toLowerCase().includes(searchTerm);
      const descriptionMatch = recipe.description
        .toLowerCase()
        .includes(searchTerm);
      const ingredientMatch = recipe.ingredients.some((ingredient) =>
        ingredient.ingredient.toLowerCase().includes(searchTerm)
      );

      return nameMatch || descriptionMatch || ingredientMatch;
    }
  },
  { time: 5_000 }
);