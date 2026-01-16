import { DrinkObject, FoodObject } from "../interfaces";

export const getIngredients = (object: FoodObject | DrinkObject | any) => {
  const keys = Object.keys(object);
  const arrayIngredients = keys.reduce((array: string[], currentKey: string) => {
    if (currentKey.includes('strIngredient') && object[currentKey]) {
      array = [...array, object[currentKey]];
    }
    return array;
  }, []);
  return arrayIngredients;
};

export const getMeasures = (object: FoodObject | DrinkObject | any) => {
  const keys = Object.keys(object);
  const arrayMeasures = keys.reduce((array: string[], currentKey: string) => {
    if (currentKey.includes('strMeasure') && object[currentKey]) {
      array = [...array, object[currentKey]];
    }
    return array;
  }, []);
  return arrayMeasures;
};

export const getFavoriteList = () => JSON.parse(localStorage.getItem('favoriteRecipes') || '{}');

export const setFavoriteList = (item: FoodObject[] | DrinkObject[]) => (
  localStorage.setItem('favoriteRecipes', JSON.stringify(item))
);

export const isFavorite = (id: string | null | undefined) => {
  if (getFavoriteList()) {
    return getFavoriteList().some((item: FoodObject | DrinkObject) => item.id === id);
  }
  setFavoriteList([] as FoodObject[] | DrinkObject[]);
  return false;
};

export const handleFavorite = (object: FoodObject | DrinkObject) => {
  console.log(object);/* 
  const { idDrink, strArea, strAlcoholic, strDrink, strDrinkThumb } = object;
  const { idMeal, strCategory, strMeal, strMealThumb } = object; */
  const { id, area, category, name, image, alcoholicOrNot, type } = object;

  if (isFavorite(id)) {
    const list = getFavoriteList()
      .filter((item: FoodObject | DrinkObject) => item.id !== id);

    setFavoriteList(list);
  } else {
    const list = {
      id,
      type,
      area: area || '',
      category,
      alcoholicOrNot: alcoholicOrNot || '',
      name,
      image,
    };
    setFavoriteList([...getFavoriteList(), list]);
  }
};

export const getDoneList = () => JSON.parse(localStorage.getItem('doneRecipes') || '');

export const attDoneList = (item: FoodObject[] | DrinkObject[]) => (
  localStorage.setItem('doneRecipes', JSON.stringify(item))
);

export const isDone = (id: string | null | undefined) => (
  getDoneList().some((item: FoodObject | DrinkObject) => item.id === id)
);

export const handleDone = (object: FoodObject | DrinkObject) => {
  const { id, area, category, name, image, alcoholicOrNot, tags, type } = object;

  if (isDone(id)) {
    const list = getDoneList()
      .filter((item: FoodObject | DrinkObject) => item.id !== id);

    attDoneList(list);
  } else {
    const list = {
      id,
      type,
      area:  area || '',
      category,
      alcoholicOrNot: alcoholicOrNot || '',
      name,
      image,
      doneDate: new Date(),
      tags: tags || [],
    };
    attDoneList([...getDoneList(), list]);
  }
};