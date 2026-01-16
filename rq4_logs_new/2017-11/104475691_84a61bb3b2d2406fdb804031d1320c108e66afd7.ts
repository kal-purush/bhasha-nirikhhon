import {Injectable} from "@angular/core";
import {Food} from "../model/food.model";
import {BehaviorSubject} from "rxjs/BehaviorSubject";

@Injectable()
export class FoodService {
  public foods$: BehaviorSubject<Food[]> = new BehaviorSubject([]);
  private _foodList: Food[];

  public loadFoods(): void {
    this._foodList = [
      Object.assign(new Food(), {
        barcode: "125478965",
        name: "banane",
        dlc: new Date(5,10,2017),
        quantity: 2,
        picture: ""
      }),
      Object.assign(new Food(), {
        barcode: "789554",
        name: "cornichon",
        dlc: new Date(5,10,2018),
        quantity: 1,
        picture: ""
      }),
      Object.assign(new Food(), {
        barcode: "127895412",
        name: "salade",
        dlc: new Date(5,10,2018),
        quantity: 1,
        picture: ""
      })
    ];
    this.foods$.next(this._foodList);
  }

  public deleteFood(food: Food): void {
    this._foodList = this._foodList.filter(f => f.barcode!=food.barcode);
    this.foods$.next(this._foodList);
  }

  public createFood(food: Food): void {
    this._foodList.push(food);
    this.foods$.next(this._foodList);
  }
}