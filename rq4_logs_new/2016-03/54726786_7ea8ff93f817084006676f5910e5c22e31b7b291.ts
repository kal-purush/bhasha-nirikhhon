import { Pipe, PipeTransform } from 'angular2/core';
import { Meal } from './meal.model';

@Pipe({
  name: "calories",
  pure: false
})

export class CaloriesPipe implements PipeTransform {
  transform(input: Meal[], args){
    var containedCalories = args[0];
    if(containedCalories === "Healthy"){
      return input.filter((meal) => {
        return meal.calories <= 300;
      });
    } else if(containedCalories === "Unhealthy") {
      return input.filter((meal) => {
        return meal.calories >= 300;
      });
    } else {
      return input;
    }
  }
}