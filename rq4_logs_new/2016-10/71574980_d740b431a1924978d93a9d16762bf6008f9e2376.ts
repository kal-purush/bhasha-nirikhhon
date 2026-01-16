import {Pipe, PipeTransform} from '@angular/core';
import {Food} from './food.model';

@Pipe ({
  name: "caloriesRank",
  pure: false
})

  export class CaloriesPipe implements PipeTransform {
    transform(input: Food[], desiredCalories) {
    var output: Food[] = [];
    if(desiredCalories === "high"){
      for( var i = 0; i < input.length; i++){
        if( input[i].calories > 500 ){
          output.push[i];
        }
      }
      return output;
    }
    else if(desiredCalories === "low"){
      for( var i = 0; i < input.length; i++){
        if( input[i].calories < 500){
          output.push(input[i]);
        }
      }
       return output;
    } else {
      return input;
    }
  }
}