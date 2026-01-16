interface IRecipe {
  recipe: RecipeItem;
  _links: {
    self: {
      href: string;
      title: string;
    };
  };
}

interface RecipeItem {
  label: string;
  images: {
    THUMBNAIL: RecipeImg;
    SMALL: RecipeImg;
    REGULAR: RecipeImg;
    LARGE: RecipeImg;
  };
  ingredientLines: string[];
  calories: number;
}

interface RecipeImg {
  url: string;
  width: number;
  height: number;
}

export { IRecipe };