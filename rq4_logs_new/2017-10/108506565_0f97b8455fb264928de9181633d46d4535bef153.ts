import {Drink} from '../model/drink.model';

export class DrinkService {

    drinksArr: Drink[] = new Array(); 

    addDrink(drink: Drink){
        this.drinksArr.push(drink);
    }

    firstDrinkConsumed(){
        let e = 100;
        for(let d of this.drinksArr){
          if(d.time < e ){
            e = d.time;
          }
        }
        return e;
      }

}