enum Macro { Carbohydrates, Protein, Fiber};

export class Recipe {
  constructor(private title: string, private numServings: number, private macros: Macro[],
              private prepTime: number, private ingredients: string[], private steps: string[]) { }

  get getRecipeTitle() {
    return this.title;
  }

  get getNumServings() {
    return this.numServings;
  }

  get getMacros() {
    return this.macros;
  }

  get getPrepTime() {
    return this.prepTime;
  }

  get getIngredients() {
    return this.ingredients;
  }

  get getSteps() {
    return this.steps;
  }

}