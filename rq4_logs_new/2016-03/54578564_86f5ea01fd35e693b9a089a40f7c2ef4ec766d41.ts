import { Pipe, PipeTransform } from 'angular2/core';
import { Keg } from './keg.model';

@Pipe({
  name: "alcohol",
  pure: false
})

export class AlcoholPipe implements PipeTransform {
  transform(input: Keg[], args){
    var alcoholContent = args[0];
    if(alcoholContent === "Strong"){
      return input.filter((keg) => {
        return keg.alcohol >= 10;
      });
    } else if (alcoholContent === "Weak"){
      return input.filter((keg) => {
        return keg.alcohol <= 10;
      });
    } else {
      return input;
    }
  }
}