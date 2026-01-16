export class ShoppingListModel {
  shoppingListId: string;
  shoppingItemText: string;

  constructor(data?: ShoppingListModel) {
    if (data != null) {
      Object.assign(this, data);
    }
  }
}