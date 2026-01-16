import { IItem } from "../interfaces/item";

export function calculateTotalCost(items: IItem[]): number {
  let total = 0;
  items.forEach((item) => {
    total = total + item.cost;
  });
  return total;
}