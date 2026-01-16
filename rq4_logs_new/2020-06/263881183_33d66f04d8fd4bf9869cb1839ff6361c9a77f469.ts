import { get, put } from './helpers/http';
import { List } from './types/ListTypes';
import { Meal } from './types/MealTypes';

const listPath = '/lists';

const getLists = () => (token: string): Promise<Array<List>> => {
  const requestPath = `${listPath}`;
  return get<Array<List>>(requestPath, token);
};

export const getListByName = (name: string) => (
  token: string
): Promise<List> => {
  const requestPath = `${listPath}/name/${name}`;
  return get<List>(requestPath, token);
};

export const updateMealInList = (listId: string, meal: Meal) => (
  token: string
): Promise<List> => {
  const requestPath = `${listPath}/${listId}/meals`;
  return put(requestPath, token, meal);
};

export default getLists;