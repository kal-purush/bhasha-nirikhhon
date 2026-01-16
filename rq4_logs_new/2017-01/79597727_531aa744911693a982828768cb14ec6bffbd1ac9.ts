import {Pipe, PipeTransform} from '@angular/core';
import {Meal} from './meal.model';

@Pipe({
  name: "calories",
  pure: false
})


export class CaloriesPipe implements PipeTransform {
  transform(input: Meal[], filterCalories){
    var calorieNum = parseInt(filterCalories)
    var output: Meal[] = [];
    for (var i = 0; i < input.length; i++) {
      if (input[i].totalCalories >= calorieNum) {
        output.push(input[i]);
      }
    }
    return output;
  }
}