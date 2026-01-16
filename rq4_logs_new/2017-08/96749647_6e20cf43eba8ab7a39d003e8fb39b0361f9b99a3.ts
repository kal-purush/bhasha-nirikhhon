
import { IShoppingListItem } from 'app/contract/IShoppingListItem';

export interface IShoppingList {
  id: number;
  name: string;
  isDone: boolean;
  items: IShoppingListItem[];
  optionalItems: IShoppingListItem[];
}