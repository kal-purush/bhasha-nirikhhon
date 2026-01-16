import { Pipe, PipeTransform } from 'angular2/core';
import { Keg } from './keg.model';

@Pipe({
  name: "low",
  pure: false
})

export class RemainingPintsPipe implements PipeTransform {
  transform(input: Keg[], args){
    var pintsRemaining = args[0];
    if (pintsRemaining === "Low"){
      return input.filter((keg) => {
        return keg.pints <= 10;
      });
    } else if (pintsRemaining === "Full"){
      return input.filter((keg) => {
        return keg.pints >= 10;
      });
    } else {
      return input;
    }
  }
}