export interface ItemCatalog {
  [id: string]: Item;
}

export interface Item {
  id: number;
  name: string;
  description: string;
  price: number;
  is_active: boolean;
  available_count: number;
}