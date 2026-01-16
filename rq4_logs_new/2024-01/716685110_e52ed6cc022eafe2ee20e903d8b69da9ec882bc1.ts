import { Component } from "../models/component";
import { IObserver } from "../models/observer-interfaces";
import { RecipesState } from "../state/recipesState";
import { RecipeComponent } from "./recipeComponent";

export class RecipesContainer implements Component, IObserver {
  private state: RecipesState;

  constructor(state: RecipesState) {
    this.state = state;
  }

  render() {
    //update recipeComponent with the new recipesDisplayed array
    const containerElement = document.getElementById("recipes-section");
    containerElement!.innerHTML = "";

    const recipeComponents: RecipeComponent[] = this.state
      .getRecipesDisplayed()
      .map((recipe) => new RecipeComponent(recipe));

    recipeComponents.forEach((component) => {
      component.render();
    });

    recipeComponents.forEach((component) => {
      containerElement?.appendChild(component.render());
    });
    return containerElement!;
  }

  update(): void {
    this.render();
    console.log("update for the recipesContainer in the recipes-section");
    console.log;
  }
}