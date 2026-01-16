import axios from 'axios';
import { drinkAPIProps, DrinkObject } from '../interfaces';

const renamingLargeNumberOfKeys = (list: drinkAPIProps[]): DrinkObject[] => {
  if (!list) {
    global.alert(
      'Sinto muito, não encontramos nenhuma receita para esses filtros.',
    );
    return [] as DrinkObject[];
  }

  const retorno = list.map((item) => {
    const {
      idDrink: id,
      strDrink: name,
      strDrinkThumb: image,
      strCategory: category,
      strAlcoholic: alcoholicOrNot,
      strInstructions: instruction,
      ...rest
    } = item;
    return {
      id,
      name,
      type: 'bebida',
      category,
      alcoholicOrNot,
      image,
      area: '',
      video: '',
      instruction,
      ...rest,
    };
  });

  return retorno;
};

const renamingSmallNumberOfKeys = (list: drinkAPIProps[]): DrinkObject[] => {
  if (!list) {
    global.alert(
      'Sinto muito, não encontramos nenhuma receita para esses filtros.',
    );
    return [];
  }
  return list.map(({ idDrink, strDrink, strDrinkThumb }) => ({
    id: idDrink,
    name: strDrink,
    image: strDrinkThumb,
    type: 'bebida',
  }));
};

export const api = axios.create({
  baseURL: 'https://www.thecocktaildb.com/api/json/v1/1',
});

export const drinkSmallAPI = async (param: string) => {
  const response = await api.get(param);

  return renamingSmallNumberOfKeys(response.data.drinks);
};

export const drinkAPI = async (param: string): Promise<DrinkObject[]> => {
  const response = await api.get(param);

  return renamingLargeNumberOfKeys(response.data.drinks);
};

export const getCategorylist = async () => {
  const response = await api.get('/list.php?c=list');

  return response.data.drinks.map(({ strCategory }: { strCategory:string}) => ({ category: strCategory }));
};

export const getRandomDrink = async () => {
  const response = await api.get('/random.php');

  return renamingLargeNumberOfKeys(response.data.drinks)[0].id;
};

export const getIngredientList = async () => {
  const response = await api.get('/list.php?i=list');
  return response.data.drinks;
};