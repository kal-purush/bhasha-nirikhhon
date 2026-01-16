import { Pipe, PipeTransform } from '@angular/core';
import { Meal} from './meal.model';

@Pipe({
  name: 'caloricity',
  pure: false
})
export class CaloricityPipe implements PipeTransform {

  transform(input: Meal[], desiredCaloricity) {
    var output: Meal[] = [];
    if(desiredCaloricity === "highCalorieMeals"){
      for(var index=0; index < input.length; index++){
        if(input[index].calories > 500){
          output.push(input[index]);
        }
      }
      return output;
    } else if(desiredCaloricity === "lowCalorieMeals"){
      for(var index=0; index < input.length; index++){
        if(input[index].calories <= 500){
          output.push(input[index]);
        }
      }
      return output;
    } else {
      return input;
    }
  }
}